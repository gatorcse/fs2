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

import fs2.Chunk

/** Tier-2 shape B: a `Chunk[Byte]` over pooled native memory.
  *
  * It is a real `Chunk`, so every byte-stream combinator, pipe and sink in fs2 accepts it,
  * and `splitAt`, `take` and `drop` slice it without copying. Sinks that know about it
  * (this module's file writer and hasher, and anything that uses `toByteBuffer` on the JVM,
  * such as fs2-io's `Files.writeAll`) read the native memory directly.
  *
  * '''Lifetime.''' The source that emits it owns a rolling window of buffers (default 4). A
  * chunk stays valid until that many more chunks have been emitted, or until the source's
  * scope closes. A stage that holds more than that, such as `chunkN` with a large `n`, a
  * `Channel`, a `Topic` or `prefetchN`, must call [[toHeap]] first. This is a convention
  * the types do not enforce: with the allocator's checks on, a stale chunk throws
  * `IllegalStateException` on bulk access (the default level) or on any access (full
  * checks); with checks off it reads whatever the buffer holds next.
  */
final class NativeChunk private[buffer] (
    /** The memory behind this chunk, exactly [[size]] bytes, shared with the source's lease. */
    val buf: NativeBuf
) extends Chunk[Byte]
    with NativeChunkPlatform {

  def size: Int = buf.size

  def apply(i: Int): Byte = buf.getByte(i)

  def copyToArray[O2 >: Byte](xs: Array[O2], start: Int): Unit =
    xs match {
      case bytes: Array[Byte] => buf.copyToArray(0, bytes, start, size)
      case _                  =>
        var i = 0
        while (i < size) {
          xs(start + i) = buf.getByte(i)
          i += 1
        }
    }

  protected def splitAtChunk_(n: Int): (Chunk[Byte], Chunk[Byte]) =
    (new NativeChunk(buf.slice(0, n)), new NativeChunk(buf.slice(n, size - n)))

  /** A heap copy that stays valid after the window moves on. */
  def toHeap: Chunk[Byte] = Chunk.array(toHeapArray)

  /** Whether the window has moved past this chunk (or its source closed). Exact. */
  def isReleased: Boolean = buf.isReleased

  private[this] def toHeapArray: Array[Byte] = {
    val a = new Array[Byte](size)
    buf.copyToArray(0, a, 0, size)
    a
  }

  override def toString: String = s"NativeChunk($buf)"
}

object NativeChunk {

  /** Copies any chunk to the heap if it is a [[NativeChunk]], so it can be held past the
    * source's window; other chunks are returned as they are.
    */
  def toHeap(c: Chunk[Byte]): Chunk[Byte] = c match {
    case n: NativeChunk => n.toHeap
    case other          => other
  }

  /** A pipe that copies native chunks to the heap. Put it before any stage that holds
    * more chunks than the source's window.
    */
  def heap[F[_]]: fs2.Pipe[F, Byte, Byte] = _.mapChunks(toHeap)
}
