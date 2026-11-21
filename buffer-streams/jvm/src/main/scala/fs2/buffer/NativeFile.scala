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
import org.typelevel.scalaccompat.annotation.unused

/** A file read and written through native buffers, with blocking calls. On the JVM a
  * `FileChannel` reading into and writing from direct views of the buffers, so the JDK
  * makes no copy. Reads and writes are sequential, and a read returns what one channel
  * read gives, so pipes and devices stream as data arrives.
  */
private[buffer] final class NativeFile private (
    private val ch: FileChannel,
    private val path: java.nio.file.Path
) {

  /** Reads into `buf` from the current position: whatever one channel read returns, at
    * most `buf.size` bytes. Returns the bytes read, or -1 at EOF.
    */
  def read(buf: NativeBuf): Int = {
    // A blocking channel returns at least one byte, or -1 at end of input.
    val r = ch.read(buf.asByteBuffer)
    if (r <= 0) -1 else r
  }

  /** Writes all of `buf` at the current position. */
  def write(buf: NativeBuf): Unit = {
    val bb = buf.asByteBuffer
    while (bb.hasRemaining) { ch.write(bb); () }
  }

  /** Copies up to `count` bytes starting at `position` to `target` inside the kernel where
    * it can (`sendfile` or `copy_file_range` on Linux). Returns the bytes copied.
    */
  def transferTo(
      position: Long,
      count: Long,
      target: NativeFile,
      @unused allocator: BufferAllocator
  ): Long =
    ch.transferTo(position, count, target.ch)

  /** The file's size, or 0 for a pipe or device. */
  def size: Long = ch.size()

  /** Whether this is the same file as `path`, if `path` exists. */
  def isSameFile(path: Path): Boolean =
    java.nio.file.Files
      .exists(path.toNioPath) && java.nio.file.Files.isSameFile(this.path, path.toNioPath)

  def close(): Unit = ch.close()
}

private[buffer] object NativeFile {

  def openRead(path: Path): NativeFile =
    new NativeFile(FileChannel.open(path.toNioPath, StandardOpenOption.READ), path.toNioPath)

  /** Opens for writing, creating the file or truncating it, or appending with `append`. */
  def openWrite(path: Path, append: Boolean): NativeFile = {
    val mode = if (append) StandardOpenOption.APPEND else StandardOpenOption.TRUNCATE_EXISTING
    new NativeFile(
      FileChannel.open(path.toNioPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE, mode),
      path.toNioPath
    )
  }
}
