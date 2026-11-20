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

import java.lang.management.ManagementFactory
import javax.management.ObjectName

/** Exposes an allocator's [[BufferAllocator.Stats]] over JMX, next to cats-effect's own
  * runtime MBeans.
  */
object BufferAllocatorMBeans {

  /** Registers `allocator` with the platform MBean server under
    * `fs2.buffer:type=BufferAllocator,name=<name>` and returns the object name. Unregister
    * it with [[unregister]] when the allocator is no longer used.
    */
  def register(allocator: BufferAllocator, name: String = "global"): ObjectName = {
    val on = new ObjectName(s"fs2.buffer:type=BufferAllocator,name=${ObjectName.quote(name)}")
    ManagementFactory.getPlatformMBeanServer.registerMBean(new Impl(allocator), on)
    on
  }

  def unregister(name: ObjectName): Unit =
    ManagementFactory.getPlatformMBeanServer.unregisterMBean(name)

  /** Every getter reads one snapshot, refreshed at most once a second, so a JMX client
    * reading all attributes takes the allocator lock once, not once per attribute. Reading
    * never polls for leaks, so `onLeak` never runs on a JMX thread.
    */
  private final class Impl(a: BufferAllocator) extends BufferAllocatorMXBean {
    private[this] var cached: BufferAllocator.Stats = null
    private[this] var cachedAt = 0L

    private[this] def s: BufferAllocator.Stats = synchronized {
      val now = System.nanoTime()
      if ((cached eq null) || now - cachedAt > 1000000000L) {
        cached = a.snapshot()
        cachedAt = now
      }
      cached
    }

    def getSlabs: Int = s.slabs
    def getSlabBytes: Long = s.slabBytes
    def getPagesInUse: Int = s.pagesInUse
    def getPagesInUseByClass: Array[Int] = s.pagesInUseByClass.toArray
    def getSizeClasses: Array[Int] = SizeClass.sizes.toArray
    def getSharedUsedSlots: Int = s.sharedUsedSlots
    def getUnpooledBuffers: Long = s.unpooledBuffers
    def getUnpooledBytes: Long = s.unpooledBytes
    def getMagazines: Int = s.magazines
    def getTrimmedBytes: Long = s.trimmedBytes
    def getLeaksDetected: Long = s.leaksDetected
    def getFollowedLeases: Int = s.followedLeases
    def getChecks: String = a.config.checks.name
    def getLeakDetection: String = a.config.leakDetection.toString
  }
}
