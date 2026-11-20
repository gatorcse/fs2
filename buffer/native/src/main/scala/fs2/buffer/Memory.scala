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

import scala.scalanative.libc.{errno, stdlib, string}
import scala.scalanative.posix.sys.mman
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

/** Scala Native memory primitives: one anonymous `mmap` per slab, and `malloc` for
  * unpooled buffers above the largest size class.
  *
  * `mmap` rather than `malloc` for slabs keeps them page-aligned and out of the C heap, so
  * returning one with `munmap` gives the memory straight back to the OS. A later step can
  * `madvise` free pages inside a live slab without unmapping it.
  */
private[buffer] object Memory {

  def allocateSlab(size: Long): SlabMemory = {
    val p = mman.mmap(
      null,
      size.toCSize,
      mman.PROT_READ | mman.PROT_WRITE,
      mman.MAP_PRIVATE | mman.MAP_ANON,
      -1,
      0L.toSize
    )
    if (p == null || p.toLong == mman.MAP_FAILED.toLong)
      throw new OutOfMemoryError(
        s"mmap of $size bytes failed: ${fromCString(string.strerror(errno.errno))}"
      )
    new NativeSlab(p, size)
  }

  def allocateUnpooled(size: Int): NativeBuf = {
    val p = stdlib.malloc(math.max(size, 1).toCSize)
    if (p == null) throw new OutOfMemoryError(s"malloc of $size bytes failed")
    new NativeBuf(p, size, size, null, -1, 0, null, true)
  }

  def freeUnpooled(p: Ptr[Byte]): Unit = stdlib.free(p)

  private final class NativeSlab(base: Ptr[Byte], val size: Long) extends SlabMemory {
    def address: Long = base.toLong
    def buf(offset: Long, size: Int, capacity: Int, page: Page, slot: Int, gen: Int): NativeBuf =
      new NativeBuf(base + offset, size, capacity, page, slot, gen, null, false)
    def free(): Unit =
      if (mman.munmap(base, this.size.toCSize) != 0)
        throw new IllegalStateException(
          s"munmap of $size bytes failed: ${fromCString(string.strerror(errno.errno))}"
        )
  }
}
