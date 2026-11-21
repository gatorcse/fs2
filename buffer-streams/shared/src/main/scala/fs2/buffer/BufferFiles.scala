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
  * Reads and writes are blocking calls on the file descriptor (a `FileChannel` on the JVM).
  *
  *   - [[readAll]] reads into ordinary heap chunks, like fs2-io's. Its chunks are safe
  *     anywhere.
  *   - [[unsafeReadAllNative]] emits [[NativeChunk]]s over the pooled memory itself, so
  *     file bytes never touch the heap, at the price of a lifetime rule the types don't
  *     enforce. For pipelines that send the bytes straight on, to [[writeAll]], a hasher or
  *     a socket.
  *   - [[writeAll]] writes either kind; native chunks go out without a copy.
  *
  * [[sendFile]] copies file to file (or device) inside the kernel, with no buffer at all.
  */
final class BufferFiles[F[_]] private (allocator: BufferAllocator)(implicit F: Sync[F]) {
  import BufferFiles._

  /** Reads `path` as heap chunks of up to `chunkSize` bytes: full-size from a regular file
    * except the last, and whatever has arrived from a pipe or device. Each read goes
    * straight into a fresh array, as fs2-io's `Files.readAll` does, so the chunks are
    * ordinary `Chunk[Byte]`s with no lifetime rules and no native memory is held. The file
    * is closed at end of input.
    */
  def readAll(path: Path, chunkSize: Int = DefaultChunkSize): Stream[F, Byte] = {
    require(chunkSize > 0, s"chunkSize must be positive, was $chunkSize")
    Stream.resource(openRead(path)).flatMap { file =>
      val readChunk: F[Option[Chunk[Byte]]] = F.blocking {
        val a = new Array[Byte](chunkSize)
        val n = file.read(a)
        if (n < 0) None else Some(Chunk.array(a, 0, n))
      }
      Stream.repeatEval(readChunk).unNoneTerminate.unchunks
    }
  }

  /** Reads `path` as native chunks of up to `chunkSize` bytes, with no copy: full-size from
    * a regular file except the last, and whatever has arrived from a pipe or device.
    *
    * '''Unsafe:''' each chunk stays valid only while `window - 1` more are emitted, and
    * never after this stream ends: the file and every buffer are released at end of input
    * (or on error or cancellation). A stage that holds chunks longer, holds them past the
    * end of the file (the last group of `chunkN`, `last`), or hands them to another fiber
    * (`merge`, `parEvalMap`, `parJoin`, `prefetch`...) must copy them with
    * [[NativeChunk.heap]] first; see [[NativeChunk]]. Use [[readAll]] unless the bytes go
    * straight on, to [[writeAll]], a hasher or a socket.
    */
  def unsafeReadAllNative(
      path: Path,
      chunkSize: Int = DefaultChunkSize,
      window: Int = DefaultWindow
  ): Stream[F, Byte] = {
    require(chunkSize > 0, s"chunkSize must be positive, was $chunkSize")
    require(window >= 1, s"window must be at least 1, was $window")
    Stream.resource((openRead(path), leaseWindow(window)).tupled).flatMap { case (file, w) =>
      def go: Pull[F, Byte, Unit] =
        Pull
          .eval(F.delay(w.lease(chunkSize)).flatMap(b => F.blocking(file.read(b)).map(b -> _)))
          .flatMap { case (b, n) =>
            if (n < 0) Pull.eval(F.delay(w.discard()))
            else
              Pull.eval(F.delay(w.push())) >>
                Pull.output(new NativeChunk(if (n == b.size) b else b.slice(0, n))) >> go
          }
      go.stream
    }
  }

  /** Writes a byte stream to `path`: native chunks from native memory, other chunks from
    * their arrays, with no copy either way and no buffer held.
    */
  def writeAll(path: Path, append: Boolean = false): Pipe[F, Byte, Nothing] =
    in =>
      Stream.resource(openWrite(path, append)).flatMap { file =>
        in.chunks.foreach(c => F.blocking(writeChunk(file, c)))
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

  private def writeChunk(file: NativeFile, c: Chunk[Byte]): Unit =
    c match {
      case n: NativeChunk => file.write(n.buf)
      case _              =>
        val sl = c.toArraySlice
        file.write(sl.values, sl.offset, sl.length)
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

  /** Native chunks kept valid at once by [[BufferFiles.unsafeReadAllNative]]. */
  final val DefaultWindow: Int = 4

  /** File streams leasing from [[BufferAllocator.global]]. */
  def apply[F[_]](implicit F: Sync[F]): BufferFiles[F] = new BufferFiles[F](BufferAllocator.global)

  /** File streams leasing from `allocator`. */
  def withAllocator[F[_]: Sync](allocator: BufferAllocator): BufferFiles[F] =
    new BufferFiles[F](allocator)
}
