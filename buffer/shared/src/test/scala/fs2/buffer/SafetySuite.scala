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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.concurrent.duration._

class SafetySuite extends munit.FunSuite {

  private def config(checks: Checks, poison: Boolean = false) =
    BufferAllocator.Config(
      slabSize = 2 * 1024 * 1024,
      idleTimeout = 1.hour,
      checks = checks,
      poison = poison,
      leakDetection = LeakDetection.Off
    )

  private def drained(alloc: BufferAllocator): Unit = {
    alloc.trim(force = true)
    val s = alloc.stats
    assertEquals(s.slabs, 0, s.toString)
    assertEquals(s.unpooledBuffers, 0L, s.toString)
  }

  // ---- use-after-release checks ----

  test("Checks.Bulk: bulk operations on a released buffer throw, single-value accessors don't") {
    val alloc = BufferAllocator(config(Checks.Bulk))
    val b = alloc.allocate(64)
    val s = b.slice(8, 8)
    alloc.release(b)
    intercept[IllegalStateException](b.toArray)
    intercept[IllegalStateException](b.fill(1))
    intercept[IllegalStateException](b.copyFromArray(new Array[Byte](4), 0, 0, 4))
    intercept[IllegalStateException](b.indexOf(0, 0, 64))
    intercept[IllegalStateException](b.slice(0, 4))
    intercept[IllegalStateException](s.toArray)
    b.getLong(0)
    s.getByte(0)
    drained(alloc)
  }

  test("Checks.Full: every accessor on a released buffer or its slices throws") {
    val alloc = BufferAllocator(config(Checks.Full))
    val b = alloc.allocate(64)
    val s = b.slice(8, 8)
    alloc.release(b)
    intercept[IllegalStateException](b.getByte(0))
    intercept[IllegalStateException](b.putLong(0, 1L))
    intercept[IllegalStateException](b.getIntLE(4))
    intercept[IllegalStateException](s.getShort(0))
    intercept[IllegalStateException](b.toArray)
    drained(alloc)
  }

  test("Checks.Paranoid: every accessor on a released buffer throws") {
    val alloc = BufferAllocator(config(Checks.Paranoid))
    val m = alloc.newMagazine()
    val b = m.allocate(64)
    m.release(b)
    intercept[IllegalStateException](b.getByte(0))
    intercept[IllegalStateException](b.toArray)
    m.close()
    drained(alloc)
  }

  test("Checks.Off: no liveness checks; the stale buffer reads whatever is there") {
    val alloc = BufferAllocator(config(Checks.Off))
    val b = alloc.allocate(64)
    alloc.release(b)
    b.toArray
    b.getLong(0)
    drained(alloc)
  }

  test("a live buffer passes every check") {
    Checks.values.foreach { c =>
      val alloc = BufferAllocator(config(c))
      val b = alloc.allocate(64)
      b.putLong(0, 42L)
      assertEquals(b.slice(0, 8).getLong(0), 42L, c.toString)
      assertEquals(b.toArray.length, 64)
      alloc.release(b)
      drained(alloc)
    }
  }

  test("checks parse from their names") {
    Checks.values.foreach(c => assertEquals(Checks.parse(c.name.toUpperCase), Some(c)))
    assertEquals(Checks.parse("nope"), None)
    assertEquals(LeakDetection.parse("sampled"), Some(LeakDetection.Sampled()))
  }

  // ---- poisoning ----

  test("poisoning fills released buffers with the poison byte") {
    val alloc = BufferAllocator(config(Checks.Off, poison = true))
    val m = alloc.newMagazine()
    val b = m.allocate(100)
    b.fill(7)
    val remote = alloc.allocate(100)
    remote.fill(7)
    m.release(b)
    alloc.release(remote)
    assert(b.toArray.forall(_ == NativeBuf.PoisonByte))
    assert(remote.toArray.forall(_ == NativeBuf.PoisonByte))
    m.close()
    drained(alloc)
  }

  test("without poisoning, released memory is left as is") {
    val alloc = BufferAllocator(config(Checks.Off))
    val b = alloc.allocate(100)
    b.fill(7)
    alloc.release(b)
    assert(b.toArray.forall(_ == 7.toByte))
    drained(alloc)
  }

  // ---- synchronous leases ----

  test("withBuffer releases on success and on exceptions") {
    val alloc = BufferAllocator(config(Checks.Full))
    val m = alloc.newMagazine()
    var seen: NativeBuf = null
    assertEquals(alloc.withBuffer(10) { b => seen = b; b.size }, 10)
    assert(seen.isReleased)
    intercept[RuntimeException](m.withBuffer(10) { b => seen = b; throw new RuntimeException })
    assert(seen.isReleased)
    m.close()
    drained(alloc)
  }

  // ---- leak detection ----

  private def leakyAllocate(alloc: BufferAllocator, n: Int): Unit =
    (0 until n).foreach(_ => alloc.allocate(1000).putInt(0, 1))

  private def awaitLeaks(
      alloc: BufferAllocator,
      reports: ConcurrentLinkedQueue[LeakReport],
      n: Int
  ): Unit = {
    val deadline = System.nanoTime() + 20.seconds.toNanos
    while (reports.size < n && System.nanoTime() < deadline) {
      System.gc()
      Thread.sleep(20)
      alloc.checkLeaks()
    }
  }

  test("leases collected without release are reported with their allocation site") {
    val reports = new ConcurrentLinkedQueue[LeakReport]
    val alloc = BufferAllocator(
      config(Checks.Bulk)
        .copy(leakDetection = LeakDetection.Paranoid, onLeak = r => { reports.add(r); () })
    )
    leakyAllocate(alloc, 5)
    awaitLeaks(alloc, reports, 5)
    assertEquals(reports.size, 5)
    assertEquals(reports.peek().size, 1000)
    assert(
      reports.peek().allocatedAt.getStackTrace.exists(_.toString.contains("leakyAllocate")),
      reports.peek().allocatedAt.getStackTrace.mkString("\n")
    )
    val s = alloc.stats
    assertEquals(s.leaksDetected, 5L)
    assertEquals(s.followedLeases, 0)
  }

  test("released leases are never reported, and sampling follows one in N") {
    val reports = new ConcurrentLinkedQueue[LeakReport]
    val alloc = BufferAllocator(
      config(Checks.Bulk)
        .copy(leakDetection = LeakDetection.Sampled(4), onLeak = r => { reports.add(r); () })
    )
    val m = alloc.newMagazine()
    val bufs = (0 until 16).map(_ => m.allocate(100))
    assertEquals(alloc.stats.followedLeases, 4)
    bufs.foreach(m.release)
    assertEquals(alloc.stats.followedLeases, 0)
    (0 until 5).foreach { _ => System.gc(); Thread.sleep(10) }
    assertEquals(alloc.checkLeaks(), 0)
    assertEquals(reports.size, 0)
    m.close()
    drained(alloc)
  }

  test("sampling intervals round up to a power of two") {
    assertEquals(LeakDetection.Sampled(1).mask, 0)
    assertEquals(LeakDetection.Sampled(100).mask, 127)
    assertEquals(LeakDetection.Sampled(128).mask, 127)
    assertEquals(LeakDetection.Off.mask, -1)
  }

  // ---- metrics ----

  test("stats count magazines, pages per size class and trimmed bytes") {
    val alloc = BufferAllocator(config(Checks.Bulk))
    val m1 = alloc.newMagazine()
    val m2 = alloc.newMagazine()
    val a = m1.allocate(100)
    val b = m2.allocate(20000)
    val c = m2.allocate(20000)
    var s = alloc.stats
    assertEquals(s.magazines, 2)
    assertEquals(s.pagesInUseByClass, Vector(1, 0, 1, 0))
    assertEquals(m2.allocations, 2L)
    m1.release(a)
    m2.release(b)
    m2.release(c)
    m1.close()
    m2.close()
    assertEquals(alloc.trim(force = true), 2L * 1024 * 1024)
    s = alloc.stats
    assertEquals(s.magazines, 0)
    assertEquals(s.pagesInUse, 0)
    assertEquals(s.trimmedBytes, 2L * 1024 * 1024)
  }
}
