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

import java.lang.foreign.{Arena, MemorySegment}

/** JVM memory primitives: one `Arena.ofShared()` per slab, sliced with `asSlice`.
  *
  * Closing a shared arena makes the JVM handshake every thread, so it happens once per
  * slab, when the slab is trimmed, never per buffer. Unpooled buffers above the largest
  * size class get their own shared arena; they are rare and large, so the close cost is
  * amortized over many bytes.
  */
private[buffer] object Memory {

  final val Alignment = 4096L

  def allocateSlab(size: Long): SlabMemory = {
    val arena = Arena.ofShared()
    val segment =
      try arena.allocate(size, Alignment)
      catch {
        case t: Throwable =>
          arena.close()
          throw t
      }
    new JvmSlab(arena, segment)
  }

  def allocateUnpooled(size: Int): NativeBuf = {
    val arena = Arena.ofShared()
    val segment =
      try arena.allocate(math.max(size, 1).toLong, 64L)
      catch {
        case t: Throwable =>
          arena.close()
          throw t
      }
    new NativeBuf(segment.asSlice(0L, size.toLong), size, null, -1, 0, null, arena)
  }

  private final class JvmSlab(arena: Arena, segment: MemorySegment) extends SlabMemory {
    val size: Long = segment.byteSize()
    def address: Long = segment.address()
    def buf(offset: Long, size: Int, capacity: Int, page: Page, slot: Int, gen: Int): NativeBuf =
      new NativeBuf(segment.asSlice(offset, size.toLong), capacity, page, slot, gen, null, null)
    def free(): Unit = arena.close()
  }
}
