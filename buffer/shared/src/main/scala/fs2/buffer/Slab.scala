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

/** A large block of native memory obtained from the platform in one piece, split into
  * equal pages. All mutable state is guarded by the owning allocator's central lock.
  */
private[buffer] final class Slab(val memory: SlabMemory, pageSize: Int) {

  val pages: Array[Page] = {
    val n = (memory.size / pageSize).toInt
    Array.tabulate(n)(i => new Page(this, i, i.toLong * pageSize, pageSize))
  }

  private[this] val freeStack: Array[Page] = pages.reverse
  private[this] var freeCount: Int = pages.length

  /** `System.nanoTime` at which the slab last became entirely free. */
  var idleSince: Long = System.nanoTime()

  def freePages: Int = freeCount
  def isEmpty: Boolean = freeCount == pages.length
  def isFull: Boolean = freeCount == 0

  def takePage(): Page = {
    freeCount -= 1
    val p = freeStack(freeCount)
    freeStack(freeCount) = null
    p
  }

  def returnPage(p: Page): Unit = {
    freeStack(freeCount) = p
    freeCount += 1
    if (isEmpty) idleSince = System.nanoTime()
  }
}
