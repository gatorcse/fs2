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

import fs2.{Chunk, Pipe}

/** Tier-2 shape A: one leased buffer of a binary stream, valid for one step.
  *
  * Deliberately not a `Chunk`, so it cannot flow by accident into code that keeps chunks.
  * It is valid until the stream that emitted it is pulled again (or its scope closes); a
  * stage that needs the bytes longer calls [[toChunk]], which copies. Sinks that release
  * as they go (this module's file writer and hasher) take it directly.
  *
  * With the allocator's checks on, using it after its step throws `IllegalStateException`
  * on bulk access (the default level) or on any access (full checks).
  */
final class Leased private[buffer] (
    /** The memory behind this lease, exactly [[size]] bytes. */
    val buf: NativeBuf
) {

  def size: Int = buf.size

  def getByte(i: Int): Byte = buf.getByte(i)

  /** A view of `[offset, offset + length)` with the same lifetime. */
  def slice(offset: Int, length: Int): Leased = new Leased(buf.slice(offset, length))

  /** A heap copy that stays valid after this step. */
  def toChunk: Chunk[Byte] = Chunk.array(buf.toArray)

  /** Whether this lease's step is over. Exact. */
  def isReleased: Boolean = buf.isReleased

  override def toString: String = s"Leased($buf)"
}

object Leased {

  /** Copies each lease to the heap, for code that wants a plain byte stream. */
  def toBytes[F[_]]: Pipe[F, Leased, Byte] = _.mapChunks(_.flatMap(_.toChunk))
}
