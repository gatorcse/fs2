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

import cats.effect.{Resource, Sync}
import cats.syntax.all._
import fs2.{Chunk, Pipe, Pull, Stream}
import fs2.io.file.Path

/** Binary file streams over pooled native buffers: the tier-2 sources and sinks.
  *
  * Each source reads with blocking calls straight into buffers leased from its own
  * [[Magazine]], so file bytes never touch the heap, and holds only a fixed number of
  * buffers however large the file. [[readAll]] emits a `Stream[F, Byte]` of
  * [[NativeChunk]]s, each valid for a rolling window of chunks, and [[writeAll]] writes
  * them back out without a copy.
  *
  * [[sendFile]] copies file to file (or device) inside the kernel, with no buffer at all.
  */
final class BufferFiles[F[_]] private (allocator: BufferAllocator)(implicit F: Sync[F]) {
  import BufferFiles._

  /** Reads `path` as native chunks of up to `chunkSize` bytes: full-size from a regular
    * file except the last, and whatever has arrived from a pipe or device. Each chunk stays
    * valid while `window - 1` more are emitted, and the last ones until the scope running
    * this stream closes; see [[NativeChunk]].
    *
    * The file is closed as soon as it is read to the end. The window's buffers stay with the
    * enclosing scope, so a stream that reads many files one after another, such as
    * `paths.flatMap(files.readAll(_))`, holds one window per file read so far until that
    * scope ends. Add `.scope` to each inner stream (`files.readAll(p).scope`) to release each
    * window when its file is done, if nothing downstream holds its chunks past that point.
    */
  def readAll(
      path: Path,
      chunkSize: Int = DefaultChunkSize,
      window: Int = DefaultWindow
  ): Stream[F, Byte] = {
    require(chunkSize > 0, s"chunkSize must be positive, was $chunkSize")
    require(window >= 1, s"window must be at least 1, was $window")
    // The window is weak: it belongs to the scope that runs this stream, not to the stream
    // itself, so buffers stay valid until the consuming stage is done with them, including
    // stages that emit after the source ends (the tail of `chunkN`, `last`). The file needs
    // no such grace, so it gets its own scope and closes at end of input.
    Stream.resourceWeak(leaseWindow(window)).flatMap { w =>
      Stream.resource(openRead(path)).flatMap { file =>
        def go: Pull[F, Byte, Unit] =
          Pull
            .eval(F.delay(w.lease(chunkSize)).flatMap(b => F.blocking(file.read(b)).map(b -> _)))
            .flatMap { case (b, n) =>
              if (n < 0) Pull.eval(F.delay(w.discard()))
              else
                Pull.eval(F.delay(w.push())) >>
                  Pull.output(new NativeChunk(if (n == b.size) b else b.slice(0, n))) >> go
            }
        go.streamNoScope
      }
    }
  }

  /** Writes a byte stream to `path`. Native chunks are written from native memory with no
    * copy; other chunks are copied into a pooled buffer first.
    */
  def writeAll(path: Path, append: Boolean = false): Pipe[F, Byte, Nothing] =
    in =>
      Stream.resource((openWrite(path, append), leaseWindow(1)).tupled).flatMap {
        case (file, scratch) => in.chunks.foreach(c => F.blocking(writeChunk(file, scratch, c)))
      }

  /** Copies all of `src` to `dst` inside the kernel where it can (`sendfile` on Linux, or
    * the JDK's `transferTo`), the download path that needs no buffer. `dst` is truncated
    * unless `append`. Returns the bytes copied.
    *
    * `src` must be a regular file: the copy runs to the size `src` has when it starts, so a
    * pipe or device copies nothing. Use [[readAll]] and [[writeAll]] for those. Copying a
    * file onto itself fails with an `IOException` rather than truncating it.
    */
  def sendFile(src: Path, dst: Path, append: Boolean = false): F[Long] =
    openRead(src)
      .evalTap { in =>
        F.blocking(in.isSameFile(dst)).flatMap { same =>
          F.raiseError[Unit](new java.io.IOException(s"sendFile: $src and $dst are the same file"))
            .whenA(same)
        }
      }
      .product(openWrite(dst, append))
      .use { case (in, out) =>
        F.blocking {
          val size = in.size
          var pos = 0L
          var stalled = false
          while (pos < size && !stalled) {
            val n = in.transferTo(pos, size - pos, out, allocator)
            if (n <= 0) stalled = true else pos += n
          }
          pos
        }
      }

  private def writeChunk(file: NativeFile, scratch: LeaseWindow, c: Chunk[Byte]): Unit =
    c match {
      case n: NativeChunk => file.write(n.buf)
      case _              =>
        val s = c.toArraySlice
        var off = 0
        while (off < s.length) {
          val len = math.min(DefaultChunkSize, s.length - off)
          val b = scratch.lease(len)
          try {
            b.copyFromArray(s.values, s.offset + off, 0, len)
            file.write(b)
          } finally scratch.discard()
          off += len
        }
    }

  private def openRead(path: Path): Resource[F, NativeFile] =
    Resource.make(F.blocking(NativeFile.openRead(path)))(f => F.blocking(f.close()))

  private def openWrite(path: Path, append: Boolean): Resource[F, NativeFile] =
    Resource.make(F.blocking(NativeFile.openWrite(path, append)))(f => F.blocking(f.close()))

  private def leaseWindow(size: Int): Resource[F, LeaseWindow] =
    Resource.make(F.delay(new LeaseWindow(allocator.newMagazine(), size)))(w => F.delay(w.close()))
}

object BufferFiles {

  /** One 64 KiB size class: large enough for disk throughput, small enough to pool. */
  final val DefaultChunkSize: Int = SizeClass.Bulk

  /** Native chunks kept valid at once by [[BufferFiles.readAll]]. */
  final val DefaultWindow: Int = 4

  /** File streams leasing from [[BufferAllocator.global]]. */
  def apply[F[_]](implicit F: Sync[F]): BufferFiles[F] = new BufferFiles[F](BufferAllocator.global)

  /** File streams leasing from `allocator`. */
  def withAllocator[F[_]: Sync](allocator: BufferAllocator): BufferFiles[F] =
    new BufferFiles[F](allocator)
}
