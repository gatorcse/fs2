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

/** The fixed size classes served from pooled pages.
  *
  * The transport picks its own buffer sizes, so a handful of classes cover it:
  *
  *   - 2 KiB: small reads on idle or chatty connections, HTTP/2 control frames
  *   - 16 KiB + 256 B: one TLS record plus header and tag, the default read size
  *   - 64 KiB: bulk reads once a connection is streaming, file I/O
  *   - 256 KiB: large writes and `writev` gather lists
  *
  * Requests larger than the biggest class are allocated directly and freed on release.
  * Every class size is a multiple of 64 so slots stay cache-line aligned.
  */
object SizeClass {

  final val Small = 2048
  final val Record = 16 * 1024 + 256
  final val Bulk = 64 * 1024
  final val Large = 256 * 1024

  /** Slot sizes, smallest first. */
  val sizes: IndexedSeq[Int] = Vector(Small, Record, Bulk, Large)

  final val Count = 4

  /** Largest size served from the pool. */
  final val MaxPooled = Large

  /** Index of the smallest class that fits `size`, or -1 if `size` is larger than every class. */
  def indexFor(size: Int): Int =
    if (size <= Small) 0
    else if (size <= Record) 1
    else if (size <= Bulk) 2
    else if (size <= Large) 3
    else -1

  /** Slot size of the class at `index`. */
  def slotSize(index: Int): Int = (index: @annotation.switch) match {
    case 0 => Small
    case 1 => Record
    case 2 => Bulk
    case 3 => Large
    case _ => throw new IndexOutOfBoundsException(s"size class $index")
  }
}
