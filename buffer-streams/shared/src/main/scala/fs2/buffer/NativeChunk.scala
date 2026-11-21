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
  * and `splitAt`, `take` and `drop` slice it without copying. This module's file writer and
  * hasher read the native memory directly. Everything that turns it into something else
  * (`toArray`, `toByteBuffer`, `toByteVector`, `toBitVector`) copies, so the result
  * outlives the chunk.
  *
  * '''Lifetime.''' The source that emits it ([[BufferFiles.unsafeReadAllNative]]) owns a
  * rolling window of buffers (default 4). A chunk stays valid while `window - 1` more
  * chunks are emitted, and is released as the source fills the next one; every chunk is
  * released when the source's stream ends. So a stage needs [[toHeap]] (or the
  * [[NativeChunk.heap]] pipe) first if it:
  *   - holds more chunks than that: `chunkN` with a large `n`, `buffer`, `compile.to(Chunk)`,
  *     `.chunks.compile.toList`, `groupWithin`, `sliding`;
  *   - emits chunks after the source has ended: the last group of `chunkN`, `bufferAll`,
  *     `groupAdjacentBy`, `split`, `takeRight`, and `text.utf8.decode` or
  *     `text.decodeWithCharset` when input ends mid-character or mid-byte-order-mark;
  *   - hands chunks to another fiber: `merge`, `parEvalMap`, `parJoin`, `prefetch`,
  *     `broadcastThrough`, `observeAsync`, `Channel`, `Topic`, fs2-io's `toInputStream`,
  *     or a stream run in the background of `concurrently`.
  *
  * Put [[NativeChunk.heap]] directly after the source, before any stage that re-chunks.
  *
  * Stages that use each chunk and move on need nothing: `map`, `evalMap`, `through` a sink,
  * `observe`, `last`, `compile.toList` (which copies the bytes out), this module's writer
  * and hasher, and fs2-io's writers.
  *
  * This is a convention the types do not enforce. What a stale chunk does depends on the
  * allocator's checks level:
  *   - Bulk (the default): copies, writes, hashing, `iterator`, `foreach` and `map` throw
  *     `IllegalStateException`. Other element access (`apply`, `head`, `last`,
  *     `zipWithIndex`, `collect`, `traverse`...) is not checked: it reads whatever the
  *     buffer holds next, and on Scala Native it can crash the process if that memory has
  *     been returned to the OS;
  *   - Full and Paranoid: every read throws;
  *   - Off: nothing is checked.
  */
final class NativeChunk private[buffer] (
    /** The memory behind this chunk, exactly [[size]] bytes, shared with the source's lease. */
    val buf: NativeBuf
) extends Chunk[Byte] {

  def size: Int = buf.size

  def apply(i: Int): Byte = buf.getByte(i)

  // Element-wise traversals check liveness once on entry, so a stale chunk fails here at
  // the default checks level rather than reading reused memory byte by byte.
  override def iterator: Iterator[Byte] = { buf.bulk(); super.iterator }

  override def foreach(f: Byte => Unit): Unit = { buf.bulk(); super.foreach(f) }

  override def map[O2](f: Byte => O2): Chunk[O2] = { buf.bulk(); super.map(f) }

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
    case n: NativeChunk            => n.toHeap
    case a: Chunk.ArraySlice[Byte] => a
    case s: Chunk.Singleton[Byte]  => s
    // Anything else may wrap native chunks (a Chunk.Queue from `++`, `buffer`, `chunkMin`),
    // so copy it whole; the copy goes through each part's checked bulk copy.
    case other => Chunk.array(other.toArray)
  }

  /** A pipe that copies native chunks to the heap. Put it before any stage that holds
    * more chunks than the source's window.
    */
  def heap[F[_]]: fs2.Pipe[F, Byte, Byte] = _.mapChunks(toHeap)
}
