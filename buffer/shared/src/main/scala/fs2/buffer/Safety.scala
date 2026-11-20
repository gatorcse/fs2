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

/** How much a [[NativeBuf]] checks that its lease is still live.
  *
  * A pooled slot stays mapped after release, so without a check a stale read returns
  * whatever the slot's next owner wrote instead of failing.
  *
  * Checks are best effort. A check and the access after it are not atomic, so a release
  * on another thread between the two goes unnoticed. Raw views (the JVM `segment` and
  * `asByteBuffer`, the Native pointer) skip the checks entirely once taken. Poisoning
  * covers a buffer's `size` bytes, not the rest of its slot.
  */
sealed abstract class Checks(private[buffer] val flags: Int, val name: String)
    extends Product
    with Serializable {
  override def toString: String = name
}

object Checks {

  /** No liveness checks. Bounds are still checked. */
  case object Off extends Checks(0, "off")

  /** Bulk operations (copies, `fill`, `indexOf`, `slice`, `toArray`, `asByteBuffer`) check
    * the lease. Single-value accessors don't, so parsers pay nothing per byte. The default.
    */
  case object Bulk extends Checks(NativeBuf.CheckBulk, "bulk")

  /** Every accessor checks the lease. For tests. */
  case object Full extends Checks(NativeBuf.CheckBulk | NativeBuf.CheckFull, "full")

  /** As [[Full]], and on the JVM every buffer gets its own shared arena, closed on release,
    * so even an access through an `asByteBuffer` view fails exactly. Much slower; for
    * tests. On Scala Native this is the same as [[Full]].
    */
  case object Paranoid
      extends Checks(NativeBuf.CheckBulk | NativeBuf.CheckFull | NativeBuf.Guarded, "paranoid")

  val values: List[Checks] = List(Off, Bulk, Full, Paranoid)

  def parse(s: String): Option[Checks] = values.find(_.name.equalsIgnoreCase(s.trim))

  /** From the `fs2.buffer.checks` system property, or [[Bulk]]. */
  def default: Checks =
    Option(System.getProperty("fs2.buffer.checks")).flatMap(parse).getOrElse(Bulk)
}

/** How many leases the leak detector follows. A followed lease records where it was
  * allocated; if it becomes unreachable without being released, the allocator reports it
  * through [[BufferAllocator.Config.onLeak]].
  *
  * Detection relies on weak references, so a leak is only seen after the GC collects the
  * lost buffer, and on Scala Native only with a GC that clears weak references (Immix or
  * Commix, not the no-op GC). Reports come from a later allocation or [[BufferAllocator.stats]]
  * call, never from inside the allocator lock.
  */
sealed abstract class LeakDetection extends Product with Serializable {

  /** Bit mask applied to an allocation counter: an allocation is followed when
    * `count & mask == 0`. -1 means never.
    */
  private[buffer] def mask: Int
}

object LeakDetection {

  case object Off extends LeakDetection {
    private[buffer] def mask = -1
  }

  /** Follows one allocation in `interval` per magazine. `interval` is rounded up to a
    * power of two.
    */
  final case class Sampled(interval: Int = 128) extends LeakDetection {
    require(interval > 0, "interval must be positive")
    private[buffer] def mask: Int = {
      val p = Integer.highestOneBit(interval)
      (if (p == interval) p else p << 1) - 1
    }
  }

  /** Follows every allocation. For tests. */
  case object Paranoid extends LeakDetection {
    private[buffer] def mask = 0
  }

  def parse(s: String): Option[LeakDetection] = s.trim.toLowerCase match {
    case "off"      => Some(Off)
    case "sampled"  => Some(Sampled())
    case "paranoid" => Some(Paranoid)
    case _          => None
  }

  /** From the `fs2.buffer.leakDetection` system property, or `Sampled(128)`. */
  def default: LeakDetection =
    Option(System.getProperty("fs2.buffer.leakDetection")).flatMap(parse).getOrElse(Sampled())
}

/** A lease that became unreachable without being released.
  *
  * @param size the requested size of the leaked buffer
  * @param allocatedAt a throwable whose stack trace shows where it was allocated
  */
final class LeakReport(val size: Int, val allocatedAt: Throwable) {
  override def toString: String = s"LeakReport(size = $size)"
}

object LeakReport {

  /** Prints the report and the allocation stack trace to standard error. */
  val printToStderr: LeakReport => Unit = { r =>
    System.err.println(
      s"fs2.buffer: LEAK: a ${r.size}-byte NativeBuf was garbage collected without being released."
    )
    r.allocatedAt.printStackTrace()
  }
}
