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

import java.lang.ref.{ReferenceQueue, WeakReference}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/** Follows sampled leases with weak references, in the manner of Netty's
  * `ResourceLeakDetector`. A followed lease that is garbage collected while still
  * registered was never released: it is reported once, with its allocation stack trace.
  *
  * Leaked slots are not reclaimed. A leaked buffer may still be reachable through a view
  * such as a `ByteBuffer`, so handing its slot to someone else would turn a leak into
  * memory corruption.
  */
private[buffer] final class LeakDetector(onLeak: LeakReport => Unit) {

  private[this] val queue = new ReferenceQueue[NativeBuf]
  private[this] val live = ConcurrentHashMap.newKeySet[LeakDetector.Tracker]()
  private[this] val leaks = new AtomicLong

  def track(buf: NativeBuf): Unit = {
    poll()
    val t = new LeakDetector.Tracker(
      buf,
      queue,
      buf.size,
      new Throwable(s"a ${buf.size}-byte NativeBuf was allocated here")
    )
    live.add(t)
    buf.tracker = t
  }

  def untrack(t: LeakDetector.Tracker): Unit = {
    live.remove(t)
    t.clear()
  }

  /** Reports every followed lease collected since the last poll. Returns how many. */
  def poll(): Int = {
    var n = 0
    var ref = queue.poll()
    while (ref ne null) {
      val t = ref.asInstanceOf[LeakDetector.Tracker]
      if (live.remove(t)) {
        n += 1
        leaks.incrementAndGet()
        try onLeak(new LeakReport(t.size, t.trace))
        catch { case _: Throwable => () }
      }
      ref = queue.poll()
    }
    n
  }

  def leaksDetected: Long = leaks.get()
  def tracked: Int = live.size
}

private[buffer] object LeakDetector {
  final class Tracker(
      buf: NativeBuf,
      q: ReferenceQueue[NativeBuf],
      val size: Int,
      val trace: Throwable
  ) extends WeakReference[NativeBuf](buf, q)
}
