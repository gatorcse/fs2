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

/** The platform primitive the allocator builds on: one contiguous block of native memory,
  * obtained and freed as a unit. Implemented with a shared FFM arena on the JVM and with
  * `mmap` on Scala Native.
  */
private[buffer] trait SlabMemory {

  /** Size in bytes. */
  def size: Long

  /** Base address, for diagnostics and tests. */
  def address: Long

  /** A buffer over `[offset, offset + size)` of this block, backed by slot `slot` of `page`. */
  def buf(offset: Long, size: Int, capacity: Int, page: Page, slot: Int): NativeBuf

  /** Returns the whole block to the OS. No buffer of it may be used afterwards. */
  def free(): Unit
}
