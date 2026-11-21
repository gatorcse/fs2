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

import java.nio.channels.FileChannel
import java.nio.file.StandardOpenOption

import fs2.io.file.Path

/** A file read and written through native buffers, with blocking calls. On the JVM a
  * `FileChannel` reading into and writing from direct views of the buffers, so the JDK
  * makes no copy. Reads and writes are sequential, so pipes and devices work too.
  */
private[buffer] final class NativeFile private (private val ch: FileChannel) {

  /** Reads into `buf` from the current position. Returns the bytes read, or -1 at EOF. */
  def read(buf: NativeBuf): Int = {
    val bb = buf.asByteBuffer
    var n = 0
    var eof = false
    // A file read may return short; keep going so chunks are full-size until EOF.
    while (bb.hasRemaining && !eof) {
      val r = ch.read(bb)
      if (r < 0) eof = true else n += r
    }
    if (n == 0 && eof) -1 else n
  }

  /** Writes all of `buf` at the current position. */
  def write(buf: NativeBuf): Unit = {
    val bb = buf.asByteBuffer
    while (bb.hasRemaining) ch.write(bb)
  }

  /** Copies up to `count` bytes starting at `position` to `target` inside the kernel where
    * it can (`sendfile` or `copy_file_range` on Linux). Returns the bytes copied.
    */
  def transferTo(position: Long, count: Long, target: NativeFile): Long =
    ch.transferTo(position, count, target.ch)

  def size: Long = ch.size()

  def close(): Unit = ch.close()
}

private[buffer] object NativeFile {

  def openRead(path: Path): NativeFile =
    new NativeFile(FileChannel.open(path.toNioPath, StandardOpenOption.READ))

  /** Opens for writing, creating the file or truncating it, or appending with `append`. */
  def openWrite(path: Path, append: Boolean): NativeFile = {
    val mode = if (append) StandardOpenOption.APPEND else StandardOpenOption.TRUNCATE_EXISTING
    new NativeFile(
      FileChannel.open(path.toNioPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE, mode)
    )
  }
}
