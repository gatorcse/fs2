/*
 * Copyright (c) 2013 Functional Streams for Scala
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of
 * this software and associated documentation files (the "Software"), to deal in
 * the Software without restriction, including without limitation the rights to
 * use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of
 * the Software, and to permit persons to whom the Software is furnished to do so,
 * subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS
 * FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR
 * COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER
 * IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package fs2.buffer

import java.io.IOException

import scala.scalanative.libc.{errno, string}
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix.{fcntl, unistd}
import scala.scalanative.posix.errno.{EINTR, EINVAL, ENOSYS}
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

import fs2.io.file.Path
import org.typelevel.scalaccompat.annotation.nowarn212

/** A file read and written through native buffers, with blocking calls: plain `read` and
  * `write` on the buffer's pointer, so nothing is copied. Reads and writes are sequential,
  * and a read returns what one `read` call gives, so pipes and devices stream as data
  * arrives.
  */
private[buffer] final class NativeFile private (private val fd: CInt) {

  /** Reads into `buf` from the current position: whatever one `read` returns, at most
    * `buf.size` bytes. Returns the bytes read, or -1 at EOF.
    */
  def read(buf: NativeBuf): Int = {
    buf.bulk()
    var r = -1
    var done = false
    while (!done) {
      r = unistd.read(fd, buf.ptr, buf.size.toCSize).toInt
      if (r >= 0) done = true
      else if (errno.errno != EINTR) throw NativeFile.error("read")
    }
    if (r == 0) -1 else r
  }

  /** Writes all of `buf` at the current position. */
  def write(buf: NativeBuf): Unit = {
    buf.bulk()
    var n = 0
    while (n < buf.size) {
      val r = unistd.write(fd, buf.ptr + n, (buf.size - n).toCSize).toInt
      if (r > 0) n += r
      else if (r == 0) throw new IOException("write: wrote 0 bytes")
      else if (errno.errno != EINTR) throw NativeFile.error("write")
    }
  }

  /** Copies up to `count` bytes starting at `position` to `target`: inside the kernel with
    * `sendfile` on Linux, through a pooled buffer elsewhere or where `sendfile` refuses the
    * pair (an `O_APPEND` target, for one). Returns the bytes copied.
    */
  def transferTo(position: Long, count: Long, target: NativeFile): Long =
    if (LinktimeInfo.isLinux) {
      val off = stackalloc[CLongLong]()
      !off = position
      var r = -1L
      var fallback = false
      while (r < 0 && !fallback) {
        r = NativeFile.linux.sendfile(target.fd, fd, off, count.toCSize).toLong
        if (r < 0) {
          val e = errno.errno
          if (e == EINVAL || e == ENOSYS) fallback = true
          else if (e != EINTR) throw NativeFile.error("sendfile")
        }
      }
      if (fallback) copyThroughBuffer(position, count, target) else r
    } else copyThroughBuffer(position, count, target)

  private def copyThroughBuffer(position: Long, count: Long, target: NativeFile): Long =
    BufferAllocator.global.withBuffer(math.min(count, SizeClass.Bulk.toLong).toInt) { buf =>
      var r = -1L
      while (r < 0) {
        r = unistd.pread(fd, buf.ptr, buf.size.toCSize, position.toSize).toLong
        if (r < 0 && errno.errno != EINTR) throw NativeFile.error("pread")
      }
      if (r > 0) target.write(buf.slice(0, r.toInt))
      r
    }

  def size: Long = {
    val cur = unistd.lseek(fd, 0, unistd.SEEK_CUR)
    val end = unistd.lseek(fd, 0, unistd.SEEK_END)
    if (cur < 0 || end < 0) throw NativeFile.error("lseek")
    unistd.lseek(fd, cur, unistd.SEEK_SET)
    end.toLong
  }

  // On Linux the descriptor is released even when close fails with EINTR, so that is not
  // an error and must not be retried.
  def close(): Unit =
    if (unistd.close(fd) != 0 && errno.errno != EINTR) throw NativeFile.error("close")
}

private[buffer] object NativeFile {

  private final val Mode = 438.toUInt // 0666, less the umask, as the JVM creates files

  def openRead(path: Path): NativeFile = open(path, fcntl.O_RDONLY)

  /** Opens for writing, creating the file or truncating it, or appending with `append`. */
  def openWrite(path: Path, append: Boolean): NativeFile =
    open(path, fcntl.O_WRONLY | fcntl.O_CREAT | (if (append) fcntl.O_APPEND else fcntl.O_TRUNC))

  private def open(path: Path, flags: CInt): NativeFile = {
    val fd = Zone.acquire { implicit z =>
      fcntl.open(toCString(path.toString), flags | fcntl.O_CLOEXEC, Mode)
    }
    if (fd < 0) throw error(s"open $path")
    new NativeFile(fd)
  }

  private[buffer] def error(what: String): IOException =
    new IOException(s"$what: ${fromCString(string.strerror(errno.errno))}")

  @nowarn212("cat=unused")
  @extern
  private[buffer] object linux {
    def sendfile(outFd: CInt, inFd: CInt, offset: Ptr[CLongLong], count: CSize): CSSize = extern
  }
}
