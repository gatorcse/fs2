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

import java.util.concurrent.atomic.AtomicInteger

/** A fixed-size run of a slab, carved into same-size slots of one size class.
  *
  * Ownership follows mimalloc: one [[Magazine]] owns the page at a time and is the only
  * one to touch the local free list, the bump pointer and `used`. Any thread may push a
  * slot onto the lock-free remote free list. The owner takes the whole remote list in one
  * swap when its local list runs dry, so the remote stack only sees pushes and a
  * take-all, which keeps it free of ABA problems.
  *
  * Slots are linked through `next`, shared by both lists: a free slot is on exactly one
  * of them, and a slot in use is on neither.
  */
private[buffer] final class Page(val slab: Slab, val index: Int, val offset: Long, val size: Int) {

  // Formatted state, written under the central lock when the page is handed to an owner.
  var sizeClass: Int = -1
  var slotSize: Int = 0
  var capacity: Int = 0
  @volatile var owner: Magazine = null
  private[this] var next: Array[Int] = Page.EmptyNext

  // Owner-only state.
  private[this] var localHead: Int = -1
  private[this] var fresh: Int = 0

  /** Slots handed out and not yet returned to the owner, from the owner's point of view.
    * Slots on the remote list still count as used until the owner reclaims them.
    */
  var used: Int = 0

  // Any thread.
  private[this] val remoteHead = new AtomicInteger(-1)

  def isFormatted: Boolean = sizeClass >= 0

  /** Prepares an unformatted page to serve `cls` for `newOwner`. Called under the central lock. */
  def format(cls: Int, newOwner: Magazine): Unit = {
    val ss = SizeClass.slotSize(cls)
    sizeClass = cls
    slotSize = ss
    capacity = size / ss
    if (next.length < capacity) next = new Array[Int](capacity)
    localHead = -1
    fresh = 0
    used = 0
    remoteHead.set(-1)
    owner = newOwner
  }

  /** Returns the page to the unformatted state. Called under the central lock, with no
    * slot outstanding.
    */
  def reset(): Unit = {
    sizeClass = -1
    slotSize = 0
    capacity = 0
    owner = null
    localHead = -1
    fresh = 0
    used = 0
    remoteHead.set(-1)
  }

  /** Owner only: takes a free slot, or returns -1 if the page is full. */
  def allocateSlot(): Int = {
    var slot = localHead
    if (slot >= 0) {
      localHead = next(slot)
    } else if (fresh < capacity) {
      slot = fresh
      fresh += 1
    } else {
      if (reclaimRemote() == 0) return -1
      slot = localHead
      localHead = next(slot)
    }
    used += 1
    slot
  }

  /** Owner only: returns a slot to the local free list. */
  def freeLocal(slot: Int): Unit = {
    next(slot) = localHead
    localHead = slot
    used -= 1
  }

  /** Any thread: returns a slot to the remote free list. */
  def freeRemote(slot: Int): Unit = {
    val n = next
    var done = false
    while (!done) {
      val head = remoteHead.get()
      n(slot) = head
      done = remoteHead.compareAndSet(head, slot)
    }
  }

  /** Owner only: moves the remote free list onto the local one. Returns how many slots
    * were reclaimed.
    */
  def reclaimRemote(): Int =
    if (remoteHead.get() < 0) 0
    else {
      val taken = remoteHead.getAndSet(-1)
      var count = 0
      var tail = taken
      var cur = taken
      while (cur >= 0) {
        count += 1
        tail = cur
        cur = next(cur)
      }
      if (count > 0) {
        next(tail) = localHead
        localHead = taken
        used -= count
      }
      count
    }

  /** Owner only: whether a call to `allocateSlot` can succeed without reclaiming. */
  def hasLocalFree: Boolean = localHead >= 0 || fresh < capacity

  /** Any thread, racy: whether remote frees are waiting. */
  def hasRemoteFree: Boolean = remoteHead.get() >= 0

  def slotOffset(slot: Int): Long = offset + slot.toLong * slotSize
}

private[buffer] object Page {
  private val EmptyNext = new Array[Int](0)
}
