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
import org.typelevel.scalaccompat.annotation.nowarn212
import scodec.bits.{BitVector, ByteVector}

/** The tier-2 binary chunk: a `Chunk[Byte]` over pooled native memory.
  *
  * It is a real `Chunk`, so every byte-stream combinator, pipe and sink in fs2 accepts it,
  * and `splitAt`, `take` and `drop` slice it without copying. Sinks that know about it
  * (this module's file writer and hasher, and anything that uses `toByteBuffer` on the JVM,
  * such as fs2-io's `Files.writeAll`) read the native memory directly.
  *
  * '''Lifetime.''' The source that emits it owns a rolling window of buffers (default 4). A
  * chunk stays valid while `window - 1` more chunks are emitted, and is released as the
  * source fills the next one; the last chunks stay valid until the source's scope closes. A
  * stage that holds more than that, such as `chunkN` with a large `n`, a `Channel`, a
  * `Topic`, `prefetchN` or `compile.to(Chunk)`, must call [[toHeap]] first.
  *
  * Anything that hands chunks to another fiber needs [[toHeap]] first too, however few it
  * holds: `parEvalMap`, `parEvalMapUnordered`, `parJoin`, `prefetch`, `concurrently`,
  * `Channel` and `Topic` run the source in a scope of its own, which can close and release
  * the last chunks while the other fiber still uses them. `merge` is safe: it waits for each
  * chunk to be consumed before pulling the next.
  *
  * This is a convention the types do not enforce. What a stale chunk does depends on the
  * allocator's checks level:
  *   - Bulk (the default): bulk reads (`toArray`, `copyToArray`, `toArraySlice`,
  *     `toByteBuffer`, `toByteVector`, writes and hashing) throw `IllegalStateException`,
  *     but element reads (`apply`, `iterator`, `foreach`, `map`) are not checked and read
  *     whatever the buffer holds next;
  *   - Full: every read throws;
  *   - Off: nothing is checked.
  *
  * `toByteVector` and `toBitVector` copy, since scodec-bits views are read later.
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
        // Through a checked bulk copy, so a stale chunk throws here as for Array[Byte].
        val a = toHeapArray
        var i = 0
        while (i < a.length) {
          xs(start + i) = a(i)
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

  /** A copy: the default is a lazy view that would read the buffer after it is released. */
  override def toByteVector[B >: Byte](implicit
      @nowarn212("cat=unused") ev: B =:= Byte
  ): ByteVector = ByteVector.view(toHeapArray)

  override def toBitVector[B >: Byte](implicit
      @nowarn212("cat=unused") ev: B =:= Byte
  ): BitVector = BitVector.view(toHeapArray)

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
