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
  * buffers however large the file. Each method comes in both tier-2 shapes:
  *
  *   - [[readAll]] / [[writeAll]]: a `Stream[F, Byte]` of [[NativeChunk]]s, valid for a
  *     rolling window of chunks;
  *   - [[readLeased]] / [[writeLeased]]: a `Stream[F, Leased]`, each valid for one step.
  *
  * [[sendFile]] copies file to file (or device) inside the kernel, with no buffer at all.
  */
final class BufferFiles[F[_]] private (allocator: BufferAllocator)(implicit F: Sync[F]) {
  import BufferFiles._

  /** Reads `path` as native chunks of `chunkSize` bytes (the last one may be shorter).
    * Each chunk stays valid while `window - 1` more are emitted, and the last ones until
    * the scope running this stream closes; see [[NativeChunk]].
    */
  def readAll(
      path: Path,
      chunkSize: Int = DefaultChunkSize,
      window: Int = DefaultWindow
  ): Stream[F, Byte] =
    read(path, chunkSize, window)(b => Pull.output(new NativeChunk(b)))

  /** Reads `path` as leases of `chunkSize` bytes (the last one may be shorter). Each lease
    * is valid until the stream is pulled again, and the last one until the scope running
    * this stream closes; see [[Leased]].
    */
  def readLeased(path: Path, chunkSize: Int = DefaultChunkSize): Stream[F, Leased] =
    read(path, chunkSize, 1)(b => Pull.output1(new Leased(b)))

  /** Writes a byte stream to `path`. Native chunks are written from native memory with no
    * copy; other chunks are copied into a pooled buffer first.
    */
  def writeAll(path: Path, append: Boolean = false): Pipe[F, Byte, Nothing] =
    in =>
      Stream.resource((openWrite(path, append), leaseWindow(1)).tupled).flatMap {
        case (file, scratch) => in.chunks.foreach(c => F.blocking(writeChunk(file, scratch, c)))
      }

  /** Writes a stream of leases to `path`, each before the next is pulled. */
  def writeLeased(path: Path, append: Boolean = false): Pipe[F, Leased, Nothing] =
    in =>
      Stream.resource(openWrite(path, append)).flatMap { file =>
        in.foreach(l => F.blocking(file.write(l.buf)))
      }

  /** Copies all of `src` to `dst` inside the kernel where it can (`sendfile` on Linux, or
    * the JDK's `transferTo`), the download path that needs no buffer. `dst` is truncated
    * unless `append`. Returns the bytes copied.
    */
  def sendFile(src: Path, dst: Path, append: Boolean = false): F[Long] =
    (openRead(src), openWrite(dst, append)).tupled.use { case (in, out) =>
      F.blocking {
        val size = in.size
        var pos = 0L
        var stalled = false
        while (pos < size && !stalled) {
          val n = in.transferTo(pos, size - pos, out)
          if (n <= 0) stalled = true else pos += n
        }
        pos
      }
    }

  private def read[O](path: Path, chunkSize: Int, window: Int)(
      emit: NativeBuf => Pull[F, O, Unit]
  ): Stream[F, O] = {
    require(chunkSize > 0, s"chunkSize must be positive, was $chunkSize")
    // Weak: the file and window belong to the scope that runs this stream, not to the
    // stream itself, so buffers stay valid until the consuming stage is done with them,
    // including stages that emit after the source ends (the tail of `chunkN`, `last`).
    Stream.resourceWeak((openRead(path), leaseWindow(window)).tupled).flatMap { case (file, w) =>
      def go: Pull[F, O, Unit] =
        Pull
          .eval(F.delay(w.lease(chunkSize)).flatMap(b => F.blocking(file.read(b)).map(b -> _)))
          .flatMap { case (b, n) =>
            if (n < 0) Pull.eval(F.delay(w.discard()))
            else Pull.eval(F.delay(w.push())) >> emit(if (n == b.size) b else b.slice(0, n)) >> go
          }
      go.streamNoScope
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
