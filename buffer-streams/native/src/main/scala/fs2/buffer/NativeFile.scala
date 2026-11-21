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
import java.nio.file.{
  AccessDeniedException,
  FileAlreadyExistsException,
  FileSystemException,
  NoSuchFileException
}

import scala.scalanative.libc.{errno, string}
import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix.{fcntl, unistd}
import scala.scalanative.posix.errno.{EACCES, EEXIST, EINTR, EINVAL, ENOENT, ENOSYS, EPERM}
import scala.scalanative.posix.sys.stat
import scala.scalanative.posix.sys.statOps._
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
    read(buf.ptr, buf.size)
  }

  /** As `read`, into `a` (which must be non-empty). */
  def read(a: Array[Byte]): Int = read(a.atUnsafe(0), a.length)

  private def read(p: Ptr[Byte], size: Int): Int = {
    var r = -1
    var done = false
    while (!done) {
      r = unistd.read(fd, p, size.toCSize).toInt
      if (r >= 0) done = true
      else if (errno.errno != EINTR) throw NativeFile.error("read")
    }
    if (r == 0) -1 else r
  }

  /** Writes all of `buf` at the current position. */
  def write(buf: NativeBuf): Unit = {
    buf.bulk()
    write(buf.ptr, buf.size)
  }

  /** Writes `length` bytes of `a` from `offset` at the current position. */
  def write(a: Array[Byte], offset: Int, length: Int): Unit =
    if (length > 0) write(a.atUnsafe(offset), length)

  private def write(p: Ptr[Byte], size: Int): Unit = {
    var n = 0
    while (n < size) {
      val r = unistd.write(fd, p + n, (size - n).toCSize).toInt
      if (r > 0) n += r
      else if (r == 0) throw new IOException("write: wrote 0 bytes")
      else if (errno.errno != EINTR) throw NativeFile.error("write")
    }
  }

  // Set once sendfile refuses this pair, so the rest of the copy skips straight to the
  // buffered path instead of paying a failed syscall per step.
  private[this] var sendfileRefused = false

  /** Copies up to `count` bytes starting at `position` to `target`: inside the kernel with
    * `sendfile` on Linux, through a pooled buffer elsewhere or where `sendfile` refuses the
    * pair (an `O_APPEND` target, for one). Returns the bytes copied.
    */
  def transferTo(
      position: Long,
      count: Long,
      target: NativeFile,
      allocator: BufferAllocator
  ): Long =
    if (LinktimeInfo.isLinux) {
      if (sendfileRefused) copyThroughBuffer(position, count, target, allocator)
      else {
        val off = stackalloc[CLongLong]()
        !off = position
        var r = -1L
        var fallback = false
        while (r < 0 && !fallback) {
          r = NativeFile.linux.sendfile(target.fd, fd, off, count.toCSize).toLong
          if (r < 0) {
            val e = errno.errno
            if (e == EINVAL || e == ENOSYS) { fallback = true; sendfileRefused = true }
            else if (e != EINTR) throw NativeFile.error("sendfile")
          }
        }
        if (fallback) copyThroughBuffer(position, count, target, allocator) else r
      }
    } else copyThroughBuffer(position, count, target, allocator)

  private def copyThroughBuffer(
      position: Long,
      count: Long,
      target: NativeFile,
      allocator: BufferAllocator
  ): Long =
    allocator.withBuffer(math.min(count, SizeClass.Bulk.toLong).toInt) { buf =>
      var r = -1L
      while (r < 0) {
        r = unistd.pread(fd, buf.ptr, buf.size.toCSize, position.toSize).toLong
        if (r < 0 && errno.errno != EINTR) throw NativeFile.error("pread")
      }
      if (r > 0) target.write(buf.slice(0, r.toInt))
      r
    }

  /** The file's size, or 0 for a pipe or device (as `FileChannel.size` is on the JVM). */
  def size: Long = {
    val st = stackalloc[stat.stat]()
    if (stat.fstat(fd, st) != 0) throw NativeFile.error("fstat")
    if (stat.S_ISREG(st.st_mode) != 0) st.st_size.toLong else 0L
  }

  /** Whether this is a regular file (not a directory, pipe or device). */
  def isRegularFile: Boolean = {
    val st = stackalloc[stat.stat]()
    if (stat.fstat(fd, st) != 0) throw NativeFile.error("fstat")
    stat.S_ISREG(st.st_mode) != 0
  }

  /** Whether this is the same file as `path`, if `path` exists. */
  def isSameFile(path: Path): Boolean = {
    val mine = stackalloc[stat.stat]()
    val theirs = stackalloc[stat.stat]()
    if (stat.fstat(fd, mine) != 0) throw NativeFile.error("fstat")
    val r = Zone.acquire(implicit z => stat.stat(toCString(path.toString), theirs))
    r == 0 && mine.st_dev == theirs.st_dev && mine.st_ino == theirs.st_ino
  }

  // On Linux the descriptor is released even when close fails with EINTR, so that is not
  // an error and must not be retried.
  def close(): Unit =
    if (NativeFile.blockingCalls.close(fd) != 0 && errno.errno != EINTR)
      throw NativeFile.error("close")
}

private[buffer] object NativeFile {

  private final val Mode = 438.toUInt // 0666, less the umask, as the JVM creates files

  def openRead(path: Path): NativeFile = open(path, fcntl.O_RDONLY)

  /** Opens for writing, creating the file or truncating it, or appending with `append`. */
  def openWrite(path: Path, append: Boolean): NativeFile =
    open(path, fcntl.O_WRONLY | fcntl.O_CREAT | (if (append) fcntl.O_APPEND else fcntl.O_TRUNC))

  private def open(path: Path, flags: CInt): NativeFile = {
    val fd = Zone.acquire { implicit z =>
      val p = toCString(path.toString)
      var fd = -1
      var done = false
      // Opening a FIFO blocks until the other end opens, so a signal can interrupt it.
      while (!done) {
        fd = blockingCalls.open(p, flags | fcntl.O_CLOEXEC, Mode)
        done = fd >= 0 || errno.errno != EINTR
      }
      fd
    }
    if (fd < 0) throw openError(path)
    new NativeFile(fd)
  }

  /** The exception the JVM's `FileChannel.open` throws for the same failure. */
  private def openError(path: Path): IOException = {
    val e = errno.errno
    val p = path.toString
    if (e == ENOENT) new NoSuchFileException(p)
    else if (e == EACCES || e == EPERM) new AccessDeniedException(p)
    else if (e == EEXIST) new FileAlreadyExistsException(p)
    else new FileSystemException(p, null, fromCString(string.strerror(e)))
  }

  private[buffer] def error(what: String): IOException =
    new IOException(s"$what: ${fromCString(string.strerror(errno.errno))}")

  /** `open` and `close` marked blocking. posixlib's are not, and opening a FIFO waits for
    * the other end (as can open or close on a hung network filesystem); an unmarked call
    * would hold up every other thread's stop-the-world GC for that long.
    */
  @nowarn212("cat=unused")
  @extern
  private object blockingCalls {
    @blocking @name("scalanative_open_m")
    def open(pathname: CString, flags: CInt, mode: CUnsignedInt): CInt = extern

    @blocking
    def close(fd: CInt): CInt = extern
  }

  @nowarn212("cat=unused")
  @extern
  private[buffer] object linux {
    // blocking: a long copy must not hold up the other threads' stop-the-world GC.
    @blocking
    def sendfile(outFd: CInt, inFd: CInt, offset: Ptr[CLongLong], count: CSize): CSSize = extern
  }
}
