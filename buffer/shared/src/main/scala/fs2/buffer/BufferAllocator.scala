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

import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}

import cats.effect.kernel.{Resource, Sync}

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._

/** A pooled allocator of native buffers, with the same structure on the JVM and on Scala
  * Native.
  *
  * Memory comes from the platform in large slabs (4 MiB by default): one shared FFM arena
  * per slab on the JVM, one `mmap` per slab on Scala Native. A slab is never closed per
  * buffer; it is split into pages, each page into slots of one [[SizeClass]], and slots are
  * pooled.
  *
  *   - Owners such as cats-effect worker threads take a [[Magazine]] with [[newMagazine]]
  *     and allocate from it with no atomics and no locks.
  *   - Other threads (the blocking pool, user threads) call [[allocate]] here, which uses a
  *     shared magazine under a lock.
  *   - Any thread may [[release]] any buffer without taking a lock. The slot goes back to the
  *     magazine that owns its page.
  *   - Empty pages return to their slab, and slabs that stay entirely free for
  *     [[BufferAllocator.Config.idleTimeout]] go back to the OS, so memory shrinks after a
  *     spike. Trimming runs opportunistically as pages are retired, and on [[trim]].
  *
  * Buffers above [[SizeClass.MaxPooled]] are allocated directly and freed on release.
  *
  * ==Leases==
  *
  * Every buffer is a lease that must be released exactly once. Library code acquires leases
  * with [[lease]] (a `Resource`) or [[withBuffer]] (`try`/`finally`), so release happens on
  * success, error and cancellation alike; bare [[allocate]] is for code that hands the lease
  * to something that will release it.
  *
  * ==Safety==
  *
  * [[BufferAllocator.Config.checks]] sets how much a buffer checks that its lease is live,
  * [[BufferAllocator.Config.poison]] fills released buffers with
  * [[NativeBuf.PoisonByte]], and [[BufferAllocator.Config.leakDetection]] follows a sample
  * of leases and reports those garbage collected without release.
  */
final class BufferAllocator private (val config: BufferAllocator.Config) {

  private[this] val lock = new Object
  private[this] val slabs = new ArrayBuffer[Slab]
  private[this] val shared = new Magazine(this)
  private[this] var lastTrim = System.nanoTime()
  private[this] val idleNanos = config.idleTimeout.toNanos

  private[this] val unpooledCount = new AtomicLong
  private[this] val unpooledBytes = new AtomicLong
  private[this] val unpooledAllocs = new AtomicLong
  private[this] val magazines = new AtomicInteger
  private[this] var trimmedBytes = 0L

  private[buffer] val guarded: Boolean =
    (config.checks.flags & NativeBuf.Guarded) != 0 && Memory.GuardedBuffers
  private[buffer] val bufFlags: Int =
    config.checks.flags | (if (config.poison) NativeBuf.Poison else 0)
  private[buffer] val leakMask: Int = config.leakDetection.mask
  private[this] val leaks = new LeakDetector(config.onLeak)

  /** A new single-owner cache. See [[Magazine]]. */
  def newMagazine(): Magazine = {
    magazines.incrementAndGet()
    new Magazine(this)
  }

  /** Allocates from the shared pool. Prefer a [[Magazine]] on hot threads, and [[lease]] or
    * [[withBuffer]] everywhere, so the lease is always released.
    */
  def allocate(size: Int): NativeBuf = {
    if (size < 0) throw new IllegalArgumentException(s"negative size $size")
    if (size > SizeClass.MaxPooled) {
      val buf = allocateUnpooled(size)
      if (leakMask >= 0 && (unpooledAllocs.getAndIncrement() & leakMask) == 0) track(buf)
      buf
    } else lock.synchronized(shared.allocate(size))
  }

  /** A buffer leased for the lifetime of the resource. It is acquired uncancelably and
    * released on success, error and cancellation.
    */
  def lease[F[_]](size: Int)(implicit F: Sync[F]): Resource[F, NativeBuf] =
    Resource.make(F.delay(allocate(size)))(buf => F.delay(release(buf)))

  /** Allocates a buffer, applies `f`, and releases the buffer however `f` exits. `f` must
    * not keep the buffer.
    */
  def withBuffer[A](size: Int)(f: NativeBuf => A): A = {
    val buf = allocate(size)
    try f(buf)
    finally release(buf)
  }

  /** Reports leases collected without release since the last check, through
    * [[BufferAllocator.Config.onLeak]]. Also runs on every followed allocation and on
    * [[stats]]. Returns how many were found.
    */
  def checkLeaks(): Int = leaks.poll()

  /** Releases a buffer from any thread, without taking a lock. */
  def release(buf: NativeBuf): Unit = {
    beforeFree(buf)
    val p = buf.page
    if (p eq null) freeUnpooled(buf)
    else p.freeRemote(buf.slot)
  }

  /** Collects the shared pool, including its empty current pages, and returns idle slabs to the OS. With `force`, every
    * entirely free slab is returned regardless of how long it has been idle. Returns the
    * number of bytes given back.
    */
  def trim(force: Boolean = false): Long = lock.synchronized {
    shared.collect(retainCurrent = false)
    trimLocked(System.nanoTime(), force)
  }

  def stats: BufferAllocator.Stats = {
    checkLeaks()
    lock.synchronized {
      val byClass = new Array[Int](SizeClass.Count)
      slabs.foreach(_.pages.foreach(p => if (p.isFormatted) byClass(p.sizeClass) += 1))
      BufferAllocator.Stats(
        slabs = slabs.length,
        slabBytes = slabs.length.toLong * config.slabSize,
        pagesInUse = byClass.sum,
        pagesInUseByClass = byClass.toVector,
        sharedUsedSlots = shared.usedSlots,
        unpooledBuffers = unpooledCount.get(),
        unpooledBytes = unpooledBytes.get(),
        magazines = magazines.get(),
        trimmedBytes = trimmedBytes,
        leaksDetected = leaks.leaksDetected,
        followedLeases = leaks.tracked
      )
    }
  }

  // ---- internal, called by magazines ----

  private[buffer] def track(buf: NativeBuf): Unit = leaks.track(buf)

  private[buffer] def magazineClosed(): Unit = { magazines.decrementAndGet(); () }

  /** Every release path runs this before the slot is freed. */
  private[buffer] def beforeFree(buf: NativeBuf): Unit = {
    buf.markReleased()
    val t = buf.tracker
    if (t ne null) {
      buf.tracker = null
      leaks.untrack(t)
    }
    buf.poison()
  }

  private[buffer] def allocateUnpooled(size: Int): NativeBuf = {
    val buf = Memory.allocateUnpooled(size, bufFlags)
    unpooledCount.incrementAndGet()
    unpooledBytes.addAndGet(size.toLong)
    buf
  }

  private[buffer] def freeUnpooled(buf: NativeBuf): Unit = {
    unpooledCount.decrementAndGet()
    unpooledBytes.addAndGet(-buf.capacity.toLong)
    buf.freeUnpooled()
  }

  private[buffer] def acquirePage(cls: Int, owner: Magazine): Page = lock.synchronized {
    // Prefer the fullest slab that still has room, so emptier slabs can drain and be trimmed.
    var best: Slab = null
    var i = 0
    while (i < slabs.length) {
      val s = slabs(i)
      if (!s.isFull && ((best eq null) || s.freePages < best.freePages)) best = s
      i += 1
    }
    if (best eq null) best = newSlab()
    val p = best.takePage()
    p.format(cls, owner)
    p
  }

  private[buffer] def retirePage(p: Page): Unit = lock.synchronized {
    p.reset()
    p.slab.returnPage(p)
    val now = System.nanoTime()
    if (now - lastTrim >= idleNanos) { trimLocked(now, force = false); () }
  }

  private[buffer] def adoptPages(pages: Iterable[Page]): Unit = lock.synchronized {
    pages.foreach(shared.adopt)
  }

  private[this] def newSlab(): Slab = {
    val total = (slabs.length + 1).toLong * config.slabSize
    if (total > config.maxPooledBytes)
      throw new BufferPoolExhaustedException(
        s"pooled native memory would exceed ${config.maxPooledBytes} bytes"
      )
    val s = new Slab(Memory.allocateSlab(config.slabSize.toLong), config.pageSize)
    slabs += s
    s
  }

  private[this] def trimLocked(now: Long, force: Boolean): Long = {
    lastTrim = now
    var freed = 0L
    var i = 0
    while (i < slabs.length) {
      val s = slabs(i)
      if (s.isEmpty && (force || now - s.idleSince >= idleNanos)) {
        slabs.remove(i)
        s.memory.free()
        freed += s.memory.size
      } else i += 1
    }
    trimmedBytes += freed
    freed
  }
}

object BufferAllocator {

  /** @param slabSize bytes requested from the platform at a time; a multiple of `pageSize`
    * @param pageSize bytes per page; at least [[SizeClass.MaxPooled]]
    * @param idleTimeout how long a slab must stay entirely free before it is returned
    * @param maxPooledBytes upper bound on slab memory; allocation past it throws
    *   [[BufferPoolExhaustedException]]
    * @param checks how much buffers check that their lease is live; defaults to the
    *   `fs2.buffer.checks` system property, else [[Checks.Bulk]]
    * @param poison fill released buffers with [[NativeBuf.PoisonByte]], so a stale read
    *   shows up as a recognizable pattern
    * @param leakDetection how many leases to follow for leaks; defaults to the
    *   `fs2.buffer.leakDetection` system property, else `Sampled(128)`
    * @param onLeak called once for each leak found
    */
  final case class Config(
      slabSize: Int = 4 * 1024 * 1024,
      pageSize: Int = 512 * 1024,
      idleTimeout: FiniteDuration = 30.seconds,
      maxPooledBytes: Long = Long.MaxValue,
      checks: Checks = Checks.default,
      poison: Boolean = false,
      leakDetection: LeakDetection = LeakDetection.default,
      onLeak: LeakReport => Unit = LeakReport.printToStderr
  ) {
    require(pageSize >= SizeClass.MaxPooled, s"pageSize must be at least ${SizeClass.MaxPooled}")
    require(pageSize % 4096 == 0, "pageSize must be a multiple of 4096")
    require(
      slabSize >= pageSize && slabSize % pageSize == 0,
      "slabSize must be a multiple of pageSize"
    )
    require(maxPooledBytes > 0, "maxPooledBytes must be positive")
  }

  /** A snapshot of the allocator.
    *
    * @param slabs slabs currently held from the OS
    * @param slabBytes bytes in those slabs
    * @param pagesInUse pages assigned to a size class
    * @param pagesInUseByClass the same, per [[SizeClass]], smallest first
    * @param sharedUsedSlots slots out from the shared pool's pages
    * @param unpooledBuffers live buffers above [[SizeClass.MaxPooled]], or every live buffer
    *   under [[Checks.Paranoid]] on the JVM
    * @param unpooledBytes bytes in those buffers
    * @param magazines magazines created and not yet closed
    * @param trimmedBytes slab bytes returned to the OS so far
    * @param leaksDetected leases reported as leaked so far
    * @param followedLeases leases the leak detector is following now
    */
  final case class Stats(
      slabs: Int,
      slabBytes: Long,
      pagesInUse: Int,
      pagesInUseByClass: Vector[Int],
      sharedUsedSlots: Int,
      unpooledBuffers: Long,
      unpooledBytes: Long,
      magazines: Int,
      trimmedBytes: Long,
      leaksDetected: Long,
      followedLeases: Int
  )

  def apply(config: Config = Config()): BufferAllocator = new BufferAllocator(config)

  /** The process-wide default allocator. */
  lazy val global: BufferAllocator = apply()
}

/** Thrown when the pool would grow past [[BufferAllocator.Config.maxPooledBytes]]. */
final class BufferPoolExhaustedException(msg: String) extends RuntimeException(msg)
