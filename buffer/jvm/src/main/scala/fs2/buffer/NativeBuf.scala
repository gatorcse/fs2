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

import java.lang.foreign.{Arena, MemorySegment, ValueLayout}
import java.nio.{ByteBuffer, ByteOrder}

/** A fixed-size region of native memory from a [[BufferAllocator]].
  *
  * On the JVM it is a slice of a `MemorySegment` belonging to a shared arena; on Scala
  * Native it is a `Ptr[Byte]` plus a length. Both platforms expose the same operations.
  *
  * Multi-byte accessors are unaligned. The unsuffixed ones are big-endian (network order),
  * and the `LE` ones little-endian, which is what 8-bytes-at-a-time scanning wants on
  * x86-64 and AArch64.
  *
  * A buffer is a lease: it must be released exactly once, through its allocator or a
  * magazine, and not used afterwards. Prefer [[BufferAllocator.lease]] or
  * [[BufferAllocator.withBuffer]], which release on every exit path. Slices share the
  * memory and the lease of their root and cannot be released themselves.
  *
  * Depending on the allocator's [[Checks]], operations on a released buffer throw
  * `IllegalStateException`; see [[Checks]] for which ones.
  */
final class NativeBuf private[buffer] (
    /** The memory behind this buffer, exactly [[size]] bytes long. */
    val segment: MemorySegment,
    /** Bytes reserved for this buffer: its slot size, at least [[size]]. */
    val capacity: Int,
    private[buffer] val page: Page,
    private[buffer] val slot: Int,
    private[buffer] val gen: Int,
    private[buffer] val root: NativeBuf,
    private[this] val arena: Arena,
    private[buffer] val flags: Int
) {
  import NativeBuf._

  /** The lease state for unpooled buffers; pooled ones use their page's generations. */
  private[this] val unpooledLease: java.util.concurrent.atomic.AtomicBoolean =
    if ((page eq null) && (root eq null)) new java.util.concurrent.atomic.AtomicBoolean else null
  private[buffer] var tracker: LeakDetector.Tracker = null

  /** Length in bytes. */
  val size: Int = segment.byteSize().toInt

  /** Address of the first byte. */
  def address: Long = segment.address()

  def isSlice: Boolean = root ne null

  /** Whether the lease has been released. Exact from any thread. A slice reports its
    * root's state.
    */
  def isReleased: Boolean =
    if (root ne null) root.isReleased
    else if (page ne null) !page.isLive(slot, gen)
    else unpooledLease.get()

  private[this] def full(): Unit =
    if ((flags & CheckFull) != 0) checkLive()

  private def bulk(): Unit =
    if ((flags & CheckBulk) != 0) checkLive()

  private[this] def checkLive(): Unit =
    if (isReleased) throw new IllegalStateException(s"$this used after release")

  def getByte(index: Int): Byte = { full(); segment.get(NativeBuf.JByte, index.toLong) }
  def putByte(index: Int, value: Byte): Unit = {
    full(); segment.set(NativeBuf.JByte, index.toLong, value)
  }

  def getShort(index: Int): Short = { full(); segment.get(NativeBuf.ShortBE, index.toLong) }
  def getShortLE(index: Int): Short = { full(); segment.get(NativeBuf.ShortLE, index.toLong) }
  def putShort(index: Int, value: Short): Unit = {
    full(); segment.set(NativeBuf.ShortBE, index.toLong, value)
  }
  def putShortLE(index: Int, value: Short): Unit = {
    full(); segment.set(NativeBuf.ShortLE, index.toLong, value)
  }

  def getInt(index: Int): Int = { full(); segment.get(NativeBuf.IntBE, index.toLong) }
  def getIntLE(index: Int): Int = { full(); segment.get(NativeBuf.IntLE, index.toLong) }
  def putInt(index: Int, value: Int): Unit = {
    full(); segment.set(NativeBuf.IntBE, index.toLong, value)
  }
  def putIntLE(index: Int, value: Int): Unit = {
    full(); segment.set(NativeBuf.IntLE, index.toLong, value)
  }

  def getLong(index: Int): Long = { full(); segment.get(NativeBuf.LongBE, index.toLong) }
  def getLongLE(index: Int): Long = { full(); segment.get(NativeBuf.LongLE, index.toLong) }
  def putLong(index: Int, value: Long): Unit = {
    full(); segment.set(NativeBuf.LongBE, index.toLong, value)
  }
  def putLongLE(index: Int, value: Long): Unit = {
    full(); segment.set(NativeBuf.LongLE, index.toLong, value)
  }

  /** Copies `length` bytes starting at `srcOffset` into `dst` at `dstOffset`. */
  def copyToArray(srcOffset: Int, dst: Array[Byte], dstOffset: Int, length: Int): Unit = {
    bulk()
    MemorySegment.copy(segment, NativeBuf.JByte, srcOffset.toLong, dst, dstOffset, length)
  }

  /** Copies `length` bytes of `src` from `srcOffset` into this buffer at `dstOffset`. */
  def copyFromArray(src: Array[Byte], srcOffset: Int, dstOffset: Int, length: Int): Unit = {
    bulk()
    MemorySegment.copy(src, srcOffset, segment, NativeBuf.JByte, dstOffset.toLong, length)
  }

  /** Copies `length` bytes from this buffer at `srcOffset` to `dst` at `dstOffset`. The
    * ranges may overlap.
    */
  def copyTo(srcOffset: Int, dst: NativeBuf, dstOffset: Int, length: Int): Unit = {
    bulk()
    dst.bulk()
    MemorySegment.copy(segment, srcOffset.toLong, dst.segment, dstOffset.toLong, length.toLong)
  }

  def fill(value: Byte): Unit = { bulk(); segment.fill(value); () }

  /** A heap copy of the whole buffer. */
  def toArray: Array[Byte] = {
    val a = new Array[Byte](size)
    copyToArray(0, a, 0, size)
    a
  }

  /** Index of the first `value` in `[from, until)`, or -1. */
  def indexOf(value: Byte, from: Int, until: Int): Int = {
    bulk()
    if (from < 0 || until > size || from > until)
      throw new IndexOutOfBoundsException(s"[$from, $until) of $size")
    var i = from
    while (i < until) {
      if (segment.get(NativeBuf.JByte, i.toLong) == value) return i
      i += 1
    }
    -1
  }

  /** A view of `[offset, offset + length)` sharing this buffer's memory and lease. */
  def slice(offset: Int, length: Int): NativeBuf = {
    bulk()
    new NativeBuf(
      segment.asSlice(offset.toLong, length.toLong),
      length,
      null,
      -1,
      0,
      if (root eq null) this else root,
      null,
      flags
    )
  }

  /** A `ByteBuffer` view for JDK APIs such as `FileChannel` and `SSLEngine`. No copy.
    *
    * The view does not check the lease: it must not outlive it. Under [[Checks.Paranoid]]
    * it fails once the lease is released, because the buffer's own arena is closed.
    */
  def asByteBuffer: ByteBuffer = { bulk(); segment.asByteBuffer() }

  override def toString: String =
    s"NativeBuf(size = $size, capacity = $capacity, address = 0x${java.lang.Long.toHexString(address)})"

  private[buffer] def markReleased(): Unit = {
    if (root ne null)
      throw new IllegalStateException("a slice cannot be released; release its root")
    val ended =
      if (page ne null) page.endLease(slot, gen) else unpooledLease.compareAndSet(false, true)
    if (!ended) throw new IllegalStateException(s"$this already released")
  }

  /** Reopens an unpooled lease whose memory could not be freed. */
  private[buffer] def unmarkReleased(): Unit = unpooledLease.set(false)

  /** Fills the buffer with [[PoisonByte]] if the allocator poisons released memory. */
  private[buffer] def poison(): Unit =
    if ((flags & Poison) != 0 && (arena eq null)) { segment.fill(PoisonByte); () }

  private[buffer] def freeUnpooled(): Unit = arena.close()
}

object NativeBuf {

  /** The byte released buffers are filled with when poisoning is on. */
  final val PoisonByte: Byte = 0xde.toByte

  private[buffer] final val CheckBulk = 1
  private[buffer] final val CheckFull = 2
  private[buffer] final val Guarded = 4
  private[buffer] final val Poison = 8

  // The accessors' layouts. Measured with AccessorBenchmark: held here they compile to the
  // same plain loads and stores as Java static finals would.
  private[buffer] val JByte: ValueLayout.OfByte = ValueLayout.JAVA_BYTE
  private[buffer] val ShortBE: ValueLayout.OfShort =
    ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN)
  private[buffer] val ShortLE: ValueLayout.OfShort =
    ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN)
  private[buffer] val IntBE: ValueLayout.OfInt =
    ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN)
  private[buffer] val IntLE: ValueLayout.OfInt =
    ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN)
  private[buffer] val LongBE: ValueLayout.OfLong =
    ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN)
  private[buffer] val LongLE: ValueLayout.OfLong =
    ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN)
}
