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

import scala.collection.mutable.ArrayBuffer

/** A single-owner allocation cache.
  *
  * A magazine owns pages of each size class and allocates from them with no atomics and
  * no locks. It must only be used by one thread at a time; handing it to another thread
  * is fine as long as the handover itself synchronizes, as a cats-effect worker's poller
  * state does when it moves across `IO.blocking`.
  *
  * Buffers may be released on any thread. A release through the owning magazine goes to
  * the page's local free list; any other release goes to the page's lock-free remote free
  * list, and the owner takes those back in batches when it runs out of local slots. Memory
  * therefore always returns to the magazine that allocated it, so per-owner memory stays
  * bounded.
  *
  * Call [[collect]] periodically (from the owner) to return empty pages, and [[close]]
  * when the owner goes away so its pages pass to the allocator's shared pool.
  */
final class Magazine private[buffer] (val allocator: BufferAllocator) {

  private[this] val current = new Array[Page](SizeClass.Count)
  private[this] val owned = Array.fill(SizeClass.Count)(new ArrayBuffer[Page])
  private[this] var closed = false
  private[this] var allocCount = 0L

  /** Allocates a buffer of exactly `size` bytes. Its capacity is the slot size of the
    * smallest fitting class, or `size` for unpooled buffers above [[SizeClass.MaxPooled]].
    */
  def allocate(size: Int): NativeBuf = {
    if (size < 0) throw new IllegalArgumentException(s"negative size $size")
    if (closed) throw new IllegalStateException("magazine is closed")
    val cls = SizeClass.indexFor(size)
    val buf =
      if (cls < 0 || allocator.guarded) allocator.allocateUnpooled(size)
      else {
        var p = current(cls)
        var slot = if (p ne null) p.allocateSlot() else -1
        if (slot < 0) {
          p = refill(cls)
          slot = p.allocateSlot()
        }
        p.slab.memory.buf(p.slotOffset(slot), size, p.slotSize, p, slot, allocator.bufFlags)
      }
    val mask = allocator.leakMask
    if (mask >= 0 && (allocCount & mask) == 0) allocator.track(buf)
    allocCount += 1
    buf
  }

  /** Allocates a buffer, applies `f`, and releases the buffer however `f` exits. For
    * synchronous code on the owning thread; `f` must not keep the buffer.
    */
  def withBuffer[A](size: Int)(f: NativeBuf => A): A = {
    val buf = allocate(size)
    try f(buf)
    finally release(buf)
  }

  /** Number of allocations made through this magazine. */
  def allocations: Long = allocCount

  /** Releases a buffer allocated by any magazine of the same allocator. */
  def release(buf: NativeBuf): Unit = {
    allocator.beforeFree(buf)
    val p = buf.page
    if (p eq null) allocator.freeUnpooled(buf)
    else if (p.owner eq this) {
      p.freeLocal(buf.slot)
      if (p.used == 0 && (p ne current(p.sizeClass))) retire(p)
    } else p.freeRemote(buf.slot)
  }

  /** Reclaims remote frees on every owned page and returns empty pages, except the
    * current page of each class, to the allocator.
    */
  def collect(): Unit = collect(retainCurrent = true)

  /** As [[collect]], and with `retainCurrent = false` also returns current pages that are
    * empty, so an idle magazine pins no memory at all.
    */
  def collect(retainCurrent: Boolean): Unit = {
    var cls = 0
    while (cls < SizeClass.Count) {
      val pages = owned(cls)
      if (!retainCurrent) {
        val cur = current(cls)
        if ((cur ne null) && { cur.reclaimRemote(); cur.used == 0 }) current(cls) = null
      }
      var i = 0
      while (i < pages.length) {
        val p = pages(i)
        p.reclaimRemote()
        if (p.used == 0 && (p ne current(cls))) {
          removeAt(pages, i)
          allocator.retirePage(p)
        } else i += 1
      }
      cls += 1
    }
  }

  /** Gives every page back: empty pages to the allocator, pages with buffers still out to
    * the allocator's shared pool, which collects them once those buffers are released.
    */
  def close(): Unit =
    if (!closed) {
      closed = true
      allocator.magazineClosed()
      var cls = 0
      while (cls < SizeClass.Count) {
        current(cls) = null
        cls += 1
      }
      collect()
      cls = 0
      while (cls < SizeClass.Count) {
        val pages = owned(cls)
        if (pages.nonEmpty) {
          allocator.adoptPages(pages)
          pages.clear()
        }
        cls += 1
      }
    }

  /** Number of slots handed out from this magazine's pages and not yet reclaimed. */
  def usedSlots: Int = {
    var n = 0
    owned.foreach(_.foreach(n += _.used))
    n
  }

  /** Number of pages this magazine owns, across all classes. */
  def ownedPages: Int = owned.map(_.length).sum

  private[buffer] def adopt(p: Page): Unit = {
    p.owner = this
    owned(p.sizeClass) += p
  }

  private[this] def refill(cls: Int): Page = {
    val pages = owned(cls)
    val cur = current(cls)
    var found: Page = null
    var i = 0
    while (i < pages.length && (found eq null)) {
      val p = pages(i)
      if ((p ne cur) && (p.hasLocalFree || p.reclaimRemote() > 0)) found = p
      i += 1
    }
    if (found eq null) {
      found = allocator.acquirePage(cls, this)
      pages += found
    }
    // The old current page is full; once it drains it is retired from release or collect.
    current(cls) = found
    found
  }

  private[this] def retire(p: Page): Unit = {
    val pages = owned(p.sizeClass)
    val i = pages.indexOf(p)
    if (i >= 0) removeAt(pages, i)
    allocator.retirePage(p)
  }

  private[this] def removeAt(pages: ArrayBuffer[Page], i: Int): Unit = {
    val last = pages.length - 1
    pages(i) = pages(last)
    pages.remove(last)
    ()
  }
}
