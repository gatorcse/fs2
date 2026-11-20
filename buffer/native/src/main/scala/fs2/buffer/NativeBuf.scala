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

import scala.scalanative.libc.string.{memcpy, memmove, memchr, memset}
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

/** A fixed-size region of native memory from a [[BufferAllocator]].
  *
  * On Scala Native it is a `Ptr[Byte]` plus a length; on the JVM it is a slice of a
  * `MemorySegment` belonging to a shared arena. Both platforms expose the same operations.
  *
  * Multi-byte accessors are unaligned. The unsuffixed ones are big-endian (network order),
  * and the `LE` ones little-endian, which is what 8-bytes-at-a-time scanning wants on
  * x86-64 and AArch64.
  *
  * Every accessor checks its bounds, as `MemorySegment` does on the JVM.
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
    /** Address of the first byte. */
    val ptr: Ptr[Byte],
    /** Length in bytes. */
    val size: Int,
    /** Bytes reserved for this buffer: its slot size, at least [[size]]. */
    val capacity: Int,
    private[buffer] val page: Page,
    private[buffer] val slot: Int,
    private[buffer] val gen: Int,
    private[buffer] val root: NativeBuf,
    private[this] val unpooled: Boolean,
    private[buffer] val flags: Int
) {
  import NativeBuf._

  /** The lease state for unpooled buffers; pooled ones use their page's generations. */
  private[this] val unpooledLease: java.util.concurrent.atomic.AtomicBoolean =
    if ((page eq null) && (root eq null)) new java.util.concurrent.atomic.AtomicBoolean else null
  private[buffer] var tracker: LeakDetector.Tracker = null

  def address: Long = ptr.toLong

  def isSlice: Boolean = root ne null

  /** Whether the lease has been released. Exact from any thread. A slice reports its
    * root's state.
    */
  def isReleased: Boolean =
    if (root ne null) root.isReleased
    else if (page ne null) !page.isLive(slot, gen)
    else unpooledLease.get()

  @inline private[this] def check(index: Int, width: Int): Unit = {
    if ((flags & CheckFull) != 0) checkLive()
    if (index < 0 || index > size - width)
      throw new IndexOutOfBoundsException(s"index $index, width $width, size $size")
  }

  @inline private def bulk(): Unit =
    if ((flags & CheckBulk) != 0) checkLive()

  private[this] def checkLive(): Unit =
    if (isReleased) throw new IllegalStateException(s"$this used after release")

  @inline private[this] def checkRange(offset: Int, length: Int, bound: Int): Unit =
    if (offset < 0 || length < 0 || offset > bound - length)
      throw new IndexOutOfBoundsException(s"offset $offset, length $length, size $bound")

  // Unaligned access, assembled from bytes. A stackalloc'd temporary plus memcpy would let
  // LLVM emit one unaligned load, but an inlined stackalloc inside a caller's loop can grow
  // the stack on every iteration. The values below are in native byte order.
  @inline private[this] def b(index: Int): Long = (ptr(index) & 0xff).toLong

  @inline private[this] def le16(index: Int): Int = (b(index) | (b(index + 1) << 8)).toInt
  @inline private[this] def le32(index: Int): Int =
    (b(index) | (b(index + 1) << 8) | (b(index + 2) << 16) | (b(index + 3) << 24)).toInt
  @inline private[this] def le64(index: Int): Long =
    (le32(index).toLong & 0xffffffffL) | (le32(index + 4).toLong << 32)

  @inline private[this] def load16(index: Int): Short = toLE16(le16(index).toShort)
  @inline private[this] def load32(index: Int): Int = toLE32(le32(index))
  @inline private[this] def load64(index: Int): Long = toLE64(le64(index))

  @inline private[this] def storeLE(index: Int, v: Long, width: Int): Unit = {
    var i = 0
    while (i < width) {
      ptr(index + i) = (v >>> (8 * i)).toByte
      i += 1
    }
  }
  @inline private[this] def store16(index: Int, v: Short): Unit =
    storeLE(index, toLE16(v).toLong, 2)
  @inline private[this] def store32(index: Int, v: Int): Unit =
    storeLE(index, toLE32(v).toLong, 4)
  @inline private[this] def store64(index: Int, v: Long): Unit = storeLE(index, toLE64(v), 8)

  def getByte(index: Int): Byte = { check(index, 1); ptr(index) }
  def putByte(index: Int, value: Byte): Unit = { check(index, 1); ptr(index) = value }

  def getShort(index: Int): Short = { check(index, 2); toBE16(load16(index)) }
  def getShortLE(index: Int): Short = { check(index, 2); toLE16(load16(index)) }
  def putShort(index: Int, value: Short): Unit = { check(index, 2); store16(index, toBE16(value)) }
  def putShortLE(index: Int, value: Short): Unit = {
    check(index, 2); store16(index, toLE16(value))
  }

  def getInt(index: Int): Int = { check(index, 4); toBE32(load32(index)) }
  def getIntLE(index: Int): Int = { check(index, 4); toLE32(load32(index)) }
  def putInt(index: Int, value: Int): Unit = { check(index, 4); store32(index, toBE32(value)) }
  def putIntLE(index: Int, value: Int): Unit = { check(index, 4); store32(index, toLE32(value)) }

  def getLong(index: Int): Long = { check(index, 8); toBE64(load64(index)) }
  def getLongLE(index: Int): Long = { check(index, 8); toLE64(load64(index)) }
  def putLong(index: Int, value: Long): Unit = { check(index, 8); store64(index, toBE64(value)) }
  def putLongLE(index: Int, value: Long): Unit = {
    check(index, 8); store64(index, toLE64(value))
  }

  /** Copies `length` bytes starting at `srcOffset` into `dst` at `dstOffset`. */
  def copyToArray(srcOffset: Int, dst: Array[Byte], dstOffset: Int, length: Int): Unit = {
    bulk()
    checkRange(srcOffset, length, size)
    checkRange(dstOffset, length, dst.length)
    if (length > 0) { memcpy(dst.atUnsafe(dstOffset), ptr + srcOffset, length.toCSize); () }
  }

  /** Copies `length` bytes of `src` from `srcOffset` into this buffer at `dstOffset`. */
  def copyFromArray(src: Array[Byte], srcOffset: Int, dstOffset: Int, length: Int): Unit = {
    bulk()
    checkRange(srcOffset, length, src.length)
    checkRange(dstOffset, length, size)
    if (length > 0) { memcpy(ptr + dstOffset, src.atUnsafe(srcOffset), length.toCSize); () }
  }

  /** Copies `length` bytes from this buffer at `srcOffset` to `dst` at `dstOffset`. The
    * ranges may overlap.
    */
  def copyTo(srcOffset: Int, dst: NativeBuf, dstOffset: Int, length: Int): Unit = {
    bulk()
    dst.bulk()
    checkRange(srcOffset, length, size)
    checkRange(dstOffset, length, dst.size)
    if (length > 0) { memmove(dst.ptr + dstOffset, ptr + srcOffset, length.toCSize); () }
  }

  def fill(value: Byte): Unit = {
    bulk()
    if (size > 0) { memset(ptr, value.toInt, size.toCSize); () }
  }

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
    if (from == until) -1
    else {
      val p = memchr(ptr + from, value.toInt & 0xff, (until - from).toCSize)
      if (p == null) -1 else (p.toLong - ptr.toLong).toInt
    }
  }

  /** A view of `[offset, offset + length)` sharing this buffer's memory and lease. */
  def slice(offset: Int, length: Int): NativeBuf = {
    bulk()
    checkRange(offset, length, size)
    new NativeBuf(
      ptr + offset,
      length,
      length,
      null,
      -1,
      0,
      if (root eq null) this else root,
      false,
      flags
    )
  }

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

  /** Fills the buffer with [[NativeBuf.PoisonByte]] if the allocator poisons released memory. */
  private[buffer] def poison(): Unit =
    if ((flags & Poison) != 0 && !unpooled && size > 0) {
      memset(ptr, PoisonByte.toInt, size.toCSize); ()
    }

  private[buffer] def freeUnpooled(): Unit =
    if (unpooled) Memory.freeUnpooled(ptr)
}

object NativeBuf {

  /** The byte released buffers are filled with when poisoning is on. */
  final val PoisonByte: Byte = 0xde.toByte

  private[buffer] final val CheckBulk = 1
  private[buffer] final val CheckFull = 2
  private[buffer] final val Guarded = 4
  private[buffer] final val Poison = 8

  private[this] val littleEndian: Boolean =
    java.nio.ByteOrder.nativeOrder() == java.nio.ByteOrder.LITTLE_ENDIAN

  @inline private[buffer] def toBE16(v: Short): Short =
    if (littleEndian) java.lang.Short.reverseBytes(v) else v
  @inline private[buffer] def toLE16(v: Short): Short =
    if (littleEndian) v else java.lang.Short.reverseBytes(v)
  @inline private[buffer] def toBE32(v: Int): Int =
    if (littleEndian) java.lang.Integer.reverseBytes(v) else v
  @inline private[buffer] def toLE32(v: Int): Int =
    if (littleEndian) v else java.lang.Integer.reverseBytes(v)
  @inline private[buffer] def toBE64(v: Long): Long =
    if (littleEndian) java.lang.Long.reverseBytes(v) else v
  @inline private[buffer] def toLE64(v: Long): Long =
    if (littleEndian) v else java.lang.Long.reverseBytes(v)
}
