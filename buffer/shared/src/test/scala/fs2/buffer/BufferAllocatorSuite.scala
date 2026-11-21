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

import java.util.concurrent.{ConcurrentLinkedQueue, CountDownLatch}
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.concurrent.duration._

import munit.ScalaCheckSuite
import org.scalacheck.{Gen, Prop}
import org.scalacheck.Prop.forAll

class BufferAllocatorSuite extends ScalaCheckSuite {

  // The thread stress tests run for tens of seconds on Native debug builds on CI.
  override def munitTimeout: Duration = 2.minutes

  // Small slabs so the tests exercise slab creation, page turnover and trimming.
  private val smallConfig =
    BufferAllocator.Config(slabSize = 2 * 1024 * 1024, pageSize = 512 * 1024, idleTimeout = 1.hour)

  private def assertDrained(alloc: BufferAllocator): Unit = {
    alloc.trim(force = true)
    val s = alloc.stats
    assertEquals(s.pagesInUse, 0, s.toString)
    assertEquals(s.slabs, 0, s.toString)
    assertEquals(s.unpooledBuffers, 0L, s.toString)
    assertEquals(s.unpooledBytes, 0L, s.toString)
  }

  /** Live buffers by start address, to check that no two overlap. */
  private final class LiveSet {
    private val byAddress = new java.util.TreeMap[java.lang.Long, NativeBuf]

    def add(b: NativeBuf): Unit = {
      val start = b.address
      val end = start + b.capacity
      val below = byAddress.floorEntry(start)
      if (below ne null)
        assert(below.getKey + below.getValue.capacity <= start, s"$b overlaps ${below.getValue}")
      val above = byAddress.ceilingEntry(start)
      if (above ne null) assert(end <= above.getKey, s"$b overlaps ${above.getValue}")
      byAddress.put(start, b)
      ()
    }

    def remove(b: NativeBuf): Unit = { byAddress.remove(b.address); () }
  }

  private def stamp(b: NativeBuf, tag: Int): Unit = b.fill(tag.toByte)

  private def checkStamp(b: NativeBuf, tag: Int): Unit = {
    val bytes = b.toArray
    var i = 0
    while (i < bytes.length) {
      if (bytes(i) != tag.toByte)
        fail(s"$b byte $i is ${bytes(i)}, expected ${tag.toByte}: memory was shared")
      i += 1
    }
  }

  // ---- model-based property test ----

  import BufferAllocatorSuite._

  private val genSize: Gen[Int] = Gen.frequency(
    8 -> Gen.choose(0, SizeClass.Small),
    6 -> Gen.choose(SizeClass.Small + 1, SizeClass.Record),
    3 -> Gen.choose(SizeClass.Record + 1, SizeClass.Bulk),
    2 -> Gen.choose(SizeClass.Bulk + 1, SizeClass.Large),
    1 -> Gen.choose(SizeClass.Large + 1, SizeClass.Large * 2)
  )

  // via: 0 and 1 are magazines, 2 is the allocator's shared pool.
  private val genOp: Gen[Op] = Gen.frequency(
    10 -> (for {
      s <- genSize
      v <- Gen.choose(0, 2)
    } yield Alloc(s, v)),
    8 -> (for {
      p <- Gen.choose(0, Int.MaxValue)
      v <- Gen.choose(0, 2)
    } yield Release(p, v)),
    1 -> Gen.const(Collect),
    1 -> Gen.const(Trim)
  )

  property("random allocate/release sequences never hand out overlapping memory") {
    forAll(Gen.listOfN(400, genOp)) { ops =>
      val alloc = BufferAllocator(smallConfig)
      val mags = Vector(alloc.newMagazine(), alloc.newMagazine())
      val live = new LiveSet
      val bufs = mutable.ArrayBuffer.empty[(NativeBuf, Int)]
      var nextTag = 1

      def release(b: NativeBuf, via: Int): Unit =
        if (via < 2) mags(via).release(b) else alloc.release(b)

      ops.foreach {
        case Alloc(size, via) =>
          val b = if (via < 2) mags(via).allocate(size) else alloc.allocate(size)
          assertEquals(b.size, size)
          assert(b.capacity >= size)
          live.add(b)
          val tag = nextTag
          nextTag += 1
          stamp(b, tag)
          bufs += (b -> tag)
        case Release(pick, via) if bufs.nonEmpty =>
          val i = pick % bufs.length
          val (b, tag) = bufs(i)
          bufs(i) = bufs.last
          bufs.remove(bufs.length - 1)
          checkStamp(b, tag)
          live.remove(b)
          release(b, via)
          assert(b.isReleased)
        case Release(_, _) => ()
        case Collect       => mags.foreach(_.collect())
        case Trim          => alloc.trim(); ()
      }

      bufs.foreach { case (b, tag) => checkStamp(b, tag); alloc.release(b) }
      mags.foreach(_.close())
      assertDrained(alloc)
      Prop.passed
    }
  }

  // ---- accessors ----

  test("primitive accessors match java.nio.ByteBuffer in both byte orders") {
    val alloc = BufferAllocator(smallConfig)
    val b = alloc.allocate(64)
    val ref = java.nio.ByteBuffer.allocate(64)
    val rnd = new scala.util.Random(42)
    (0 until 200).foreach { _ =>
      val i = rnd.nextInt(56)
      rnd.nextInt(7) match {
        case 0 =>
          val v = rnd.nextLong(); b.putLong(i, v);
          ref.order(java.nio.ByteOrder.BIG_ENDIAN).putLong(i, v)
        case 1 =>
          val v = rnd.nextLong(); b.putLongLE(i, v);
          ref.order(java.nio.ByteOrder.LITTLE_ENDIAN).putLong(i, v)
        case 2 =>
          val v = rnd.nextInt(); b.putInt(i, v);
          ref.order(java.nio.ByteOrder.BIG_ENDIAN).putInt(i, v)
        case 3 =>
          val v = rnd.nextInt(); b.putIntLE(i, v);
          ref.order(java.nio.ByteOrder.LITTLE_ENDIAN).putInt(i, v)
        case 4 =>
          val v = rnd.nextInt().toShort; b.putShort(i, v);
          ref.order(java.nio.ByteOrder.BIG_ENDIAN).putShort(i, v)
        case 5 =>
          val v = rnd.nextInt().toShort; b.putShortLE(i, v);
          ref.order(java.nio.ByteOrder.LITTLE_ENDIAN).putShort(i, v)
        case _ =>
          val v = rnd.nextInt().toByte; b.putByte(i, v); ref.put(i, v)
      }
      val j = rnd.nextInt(56)
      assertEquals(b.getLong(j), ref.order(java.nio.ByteOrder.BIG_ENDIAN).getLong(j))
      assertEquals(b.getLongLE(j), ref.order(java.nio.ByteOrder.LITTLE_ENDIAN).getLong(j))
      assertEquals(b.getInt(j), ref.order(java.nio.ByteOrder.BIG_ENDIAN).getInt(j))
      assertEquals(b.getIntLE(j), ref.order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt(j))
      assertEquals(b.getShort(j), ref.order(java.nio.ByteOrder.BIG_ENDIAN).getShort(j))
      assertEquals(b.getShortLE(j), ref.order(java.nio.ByteOrder.LITTLE_ENDIAN).getShort(j))
      assertEquals(b.getByte(j), ref.get(j))
    }
    assert(java.util.Arrays.equals(b.toArray, ref.array()))
    alloc.release(b)
    assertDrained(alloc)
  }

  test("bulk copies, slices and indexOf") {
    val alloc = BufferAllocator(smallConfig)
    val a = alloc.allocate(100)
    val c = alloc.allocate(100)
    val src = Array.tabulate[Byte](100)(i => i.toByte)
    a.copyFromArray(src, 10, 5, 50)
    val out = new Array[Byte](50)
    a.copyToArray(5, out, 0, 50)
    assert(java.util.Arrays.equals(out, src.slice(10, 60)))

    a.copyTo(5, c, 20, 50)
    assertEquals(c.getByte(20), 10.toByte)
    assertEquals(c.getByte(69), 59.toByte)

    val s = a.slice(5, 50)
    assertEquals(s.size, 50)
    assertEquals(s.getByte(0), 10.toByte)
    assertEquals(s.address, a.address + 5)
    assertEquals(s.slice(10, 5).getByte(0), 20.toByte)
    assertEquals(a.indexOf(30.toByte, 0, 100), 25)
    assertEquals(a.indexOf(30.toByte, 26, 100), -1)
    assertEquals(s.indexOf(30.toByte, 0, 50), 20)

    intercept[IndexOutOfBoundsException](a.getLong(93))
    intercept[IndexOutOfBoundsException](a.getByte(-1))
    intercept[IndexOutOfBoundsException](s.getByte(50))
    intercept[IndexOutOfBoundsException](a.copyToArray(90, out, 0, 20))
    intercept[IndexOutOfBoundsException](a.slice(90, 20))

    intercept[IllegalStateException](alloc.release(s))
    alloc.release(a)
    assert(s.isReleased)
    intercept[IllegalStateException](alloc.release(a))
    alloc.release(c)
    assertDrained(alloc)
  }

  test("zero-length and unpooled buffers") {
    val alloc = BufferAllocator(smallConfig)
    val z = alloc.allocate(0)
    assertEquals(z.size, 0)
    assertEquals(z.toArray.length, 0)
    val big = alloc.allocate(SizeClass.Large + 1)
    assertEquals(big.capacity, SizeClass.Large + 1)
    big.putLong(big.size - 8, 7L)
    assertEquals(big.getLong(big.size - 8), 7L)
    assertEquals(alloc.stats.unpooledBuffers, 1L)
    alloc.release(z)
    alloc.release(big)
    intercept[IllegalStateException](alloc.release(big))
    assertDrained(alloc)
  }

  // ---- size classes and pages ----

  test("buffers get the smallest fitting size class") {
    val alloc = BufferAllocator(smallConfig)
    val m = alloc.newMagazine()
    List(
      1 -> SizeClass.Small,
      2048 -> SizeClass.Small,
      2049 -> SizeClass.Record,
      16640 -> SizeClass.Record,
      16641 -> SizeClass.Bulk,
      65537 -> SizeClass.Large
    )
      .foreach { case (size, cap) =>
        val b = m.allocate(size)
        assertEquals(b.capacity, cap, s"size $size")
        assertEquals(b.address % 64, 0L, s"size $size")
        m.release(b)
      }
    m.close()
    assertDrained(alloc)
  }

  test("a released slot is reused before the page grows") {
    val alloc = BufferAllocator(smallConfig)
    val m = alloc.newMagazine()
    val a = m.allocate(1000)
    val addr = a.address
    m.release(a)
    val b = m.allocate(1000)
    assertEquals(b.address, addr)
    m.release(b)
    m.close()
    assertDrained(alloc)
  }

  test("pages turn over and empty pages go back to their slab") {
    val alloc = BufferAllocator(smallConfig)
    val m = alloc.newMagazine()
    // 512 KiB pages hold two 256 KiB slots; 8 buffers need 4 pages.
    val bufs = (0 until 8).map(_ => m.allocate(SizeClass.Large))
    assertEquals(m.ownedPages, 4)
    assertEquals(alloc.stats.pagesInUse, 4)
    bufs.foreach(m.release)
    // Everything but the current page is retired as soon as it drains.
    assertEquals(m.ownedPages, 1)
    m.collect()
    assertEquals(alloc.stats.pagesInUse, 1)
    m.close()
    assertEquals(alloc.stats.pagesInUse, 0)
    assertDrained(alloc)
  }

  // ---- trimming and limits ----

  test("trim returns idle slabs only after the idle timeout, or when forced") {
    val alloc = BufferAllocator(smallConfig)
    val m = alloc.newMagazine()
    val bufs = (0 until 16).map(_ => m.allocate(SizeClass.Large))
    assertEquals(alloc.stats.slabs, 2)
    bufs.foreach(m.release)
    m.close()
    assertEquals(alloc.trim(), 0L)
    assertEquals(alloc.stats.slabs, 2)
    assertEquals(alloc.trim(force = true), 4L * 1024 * 1024)
    assertEquals(alloc.stats.slabs, 0)
  }

  test("trim returns a slab once it has been idle for the timeout") {
    val alloc = BufferAllocator(smallConfig.copy(idleTimeout = 200.millis))
    val m = alloc.newMagazine()
    val bufs = (0 until 4).map(_ => m.allocate(SizeClass.Large))
    bufs.foreach(m.release)
    m.close()
    assertEquals(alloc.trim(), 0L)
    Thread.sleep(400)
    assertEquals(alloc.trim(), 2L * 1024 * 1024)
    assertEquals(alloc.stats.slabs, 0)
  }

  test("with a zero idle timeout, retiring pages trims empty slabs on its own") {
    val alloc = BufferAllocator(smallConfig.copy(idleTimeout = Duration.Zero))
    val m = alloc.newMagazine()
    val bufs = (0 until 16).map(_ => m.allocate(SizeClass.Large))
    bufs.foreach(m.release)
    m.close()
    assertEquals(alloc.stats.slabs, 0)
  }

  test("the pool refuses to grow past maxPooledBytes") {
    val alloc = BufferAllocator(smallConfig.copy(maxPooledBytes = 2L * 1024 * 1024))
    val m = alloc.newMagazine()
    val bufs = (0 until 8).map(_ => m.allocate(SizeClass.Large))
    intercept[BufferPoolExhaustedException](m.allocate(SizeClass.Large))
    bufs.foreach(m.release)
    m.close()
    assertDrained(alloc)
  }

  test("config validation") {
    intercept[IllegalArgumentException](BufferAllocator.Config(pageSize = 128 * 1024))
    intercept[IllegalArgumentException](
      BufferAllocator.Config(slabSize = 3 * 512 * 1024 + 4096)
    )
  }

  // ---- cross-thread release ----

  private def onThread(body: => Unit): Thread = {
    val failure = new AtomicReference[Throwable]
    val t = new Thread(() =>
      try body
      catch { case e: Throwable => failure.set(e) }
    )
    t.start()
    t.join()
    if (failure.get() ne null) throw failure.get()
    t
  }

  test("buffers released on another thread return to the owning magazine") {
    val alloc = BufferAllocator(smallConfig)
    val m = alloc.newMagazine()
    val bufs = (0 until 400).map(_ => m.allocate(1000))
    val pagesBefore = m.ownedPages
    onThread(bufs.foreach(alloc.release))
    assertEquals(m.usedSlots, 400) // not reclaimed yet: the owner hasn't looked
    // The owner reclaims the remote frees instead of taking new pages.
    val again = (0 until 400).map(_ => m.allocate(1000))
    assertEquals(m.ownedPages, pagesBefore)
    assertEquals(alloc.stats.pagesInUse, pagesBefore)
    // Handing the magazine itself to another thread is fine when the handover synchronizes.
    onThread(again.foreach(m.release))
    m.collect()
    assertEquals(m.usedSlots, 0)
    m.close()
    assertDrained(alloc)
  }

  test("closing a magazine with buffers outstanding hands its pages to the shared pool") {
    val alloc = BufferAllocator(smallConfig)
    val m = alloc.newMagazine()
    val bufs = (0 until 10).map(_ => m.allocate(5000))
    m.close()
    intercept[IllegalStateException](m.allocate(1))
    assertEquals(alloc.stats.sharedUsedSlots, 10)
    bufs.foreach(alloc.release)
    alloc.trim()
    assertEquals(alloc.stats.sharedUsedSlots, 0)
    assertDrained(alloc)
  }

  test("racing releases of one buffer: exactly one wins, the slot is freed once") {
    val alloc = BufferAllocator(smallConfig)
    val m = alloc.newMagazine()
    (0 until 2000).foreach { _ =>
      val b = m.allocate(100)
      val start = new CountDownLatch(1)
      val wins = new java.util.concurrent.atomic.AtomicInteger
      val threads = (0 until 2).map { _ =>
        new Thread(() => {
          start.await()
          try { alloc.release(b); wins.incrementAndGet(); () }
          catch { case _: IllegalStateException => () }
        })
      }
      threads.foreach(_.start())
      start.countDown()
      threads.foreach(_.join())
      assertEquals(wins.get(), 1)
      assert(b.isReleased)
    }
    m.collect()
    assertEquals(m.usedSlots, 0)
    m.close()
    assertDrained(alloc)
  }

  test("a stale buffer can never release a later lease of the same slot") {
    val alloc = BufferAllocator(smallConfig)
    val m = alloc.newMagazine()
    val a = m.allocate(100)
    m.release(a)
    val b = m.allocate(100)
    assertEquals(b.address, a.address)
    assert(a.isReleased)
    assert(!b.isReleased)
    intercept[IllegalStateException](m.release(a))
    intercept[IllegalStateException](alloc.release(a))
    assert(!b.isReleased)
    m.release(b)
    m.close()
    assertDrained(alloc)
  }

  test("owners, the shared pool, closes and trims all running at once") {
    val alloc = BufferAllocator(smallConfig)
    val queue = new ConcurrentLinkedQueue[(NativeBuf, Int)]
    val stop = new java.util.concurrent.atomic.AtomicBoolean(false)
    val failure = new AtomicReference[Throwable]
    def guard(body: => Unit): Thread = new Thread(() =>
      try body
      catch { case e: Throwable => failure.compareAndSet(null, e); () }
    )
    def checkAndRelease(e: (NativeBuf, Int), release: NativeBuf => Unit): Unit = {
      val (x, tag) = e
      // Samples every 61st byte and the last one: any other owner of the slot fills it
      // whole, and reading every byte is too slow for Native's debug builds.
      var i = 0
      while (i < x.size) {
        if (x.getByte(i) != tag.toByte || x.getByte(x.size - 1) != tag.toByte)
          throw new AssertionError(s"$x was overwritten while live")
        i += 61
      }
      release(x)
    }
    val owners = (0 until 3).map { t =>
      guard {
        val rnd = new scala.util.Random(t.toLong)
        var round = 0
        while (round < 20) {
          val m = alloc.newMagazine()
          var i = 0
          while (i < 500) {
            val b = m.allocate(rnd.nextInt(SizeClass.Bulk) + 1)
            val tag = rnd.nextInt(255) + 1
            b.fill(tag.toByte)
            queue.add(b -> tag)
            val e = queue.poll()
            if (e ne null) checkAndRelease(e, m.release)
            i += 1
          }
          m.close() // pages with buffers still queued move to the shared pool
          round += 1
        }
      }
    }
    val sharedUser = guard {
      val rnd = new scala.util.Random(99)
      while (!stop.get()) {
        val b = alloc.allocate(rnd.nextInt(SizeClass.Record) + 1)
        val tag = rnd.nextInt(255) + 1
        b.fill(tag.toByte)
        queue.add(b -> tag)
        val e = queue.poll()
        if (e ne null) checkAndRelease(e, alloc.release)
      }
    }
    val trimmer = guard {
      // Sleeps between trims: a trimmer spinning on the lock starves the others on Native.
      while (!stop.get()) { alloc.trim(); Thread.sleep(1) }
    }
    (owners :+ sharedUser :+ trimmer).foreach(_.start())
    owners.foreach(_.join())
    stop.set(true)
    sharedUser.join()
    trimmer.join()
    if (failure.get() ne null) throw failure.get()
    var e = queue.poll()
    while (e ne null) { checkAndRelease(e, alloc.release); e = queue.poll() }
    assertDrained(alloc)
  }

  test("concurrent owners exchanging buffers never share memory or leak") {
    val alloc = BufferAllocator(smallConfig)
    val threads = 4
    val perThread = 20000
    val queue = new ConcurrentLinkedQueue[(NativeBuf, Int)]
    val start = new CountDownLatch(1)
    val failure = new AtomicReference[Throwable]

    val workers = (0 until threads).map { t =>
      new Thread(() =>
        try {
          val m = alloc.newMagazine()
          val rnd = new scala.util.Random(t.toLong)
          start.await()
          var i = 0
          while (i < perThread) {
            val size = rnd.nextInt(4) match {
              case 0 => rnd.nextInt(SizeClass.Small) + 1
              case 1 => rnd.nextInt(SizeClass.Record) + 1
              case 2 => rnd.nextInt(SizeClass.Bulk) + 1
              case _ => 8
            }
            val b = m.allocate(size)
            val tag = (t << 24) | i
            if (b.size >= 8) { b.putInt(0, tag); b.putInt(b.size - 4, tag) }
            queue.add(b -> tag)
            // Release roughly as many as we allocate, mostly other owners' buffers.
            var k = 0
            while (k < 1 + rnd.nextInt(2)) {
              val e = queue.poll()
              if (e ne null) {
                val (x, xt) = e
                if (x.size >= 8) {
                  if (x.getInt(0) != xt || x.getInt(x.size - 4) != xt)
                    throw new AssertionError(s"$x was overwritten while live")
                }
                if (rnd.nextBoolean()) m.release(x) else alloc.release(x)
              }
              k += 1
            }
            i += 1
          }
          m.close()
        } catch { case e: Throwable => failure.compareAndSet(null, e); () }
      )
    }
    workers.foreach(_.start())
    start.countDown()
    workers.foreach(_.join())
    if (failure.get() ne null) throw failure.get()

    var e = queue.poll()
    while (e ne null) { alloc.release(e._1); e = queue.poll() }
    alloc.trim()
    assertEquals(alloc.stats.sharedUsedSlots, 0)
    assertDrained(alloc)
  }
}

object BufferAllocatorSuite {
  private sealed trait Op
  private final case class Alloc(size: Int, via: Int) extends Op
  private final case class Release(pick: Int, via: Int) extends Op
  private case object Collect extends Op
  private case object Trim extends Op
}
