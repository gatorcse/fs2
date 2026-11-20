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

class NativeBufJvmSuite extends munit.FunSuite {

  test("asByteBuffer is a direct, zero-copy view") {
    val alloc = BufferAllocator()
    val b = alloc.allocate(32)
    val bb = b.asByteBuffer
    assert(bb.isDirect)
    assertEquals(bb.capacity(), 32)
    bb.putLong(8, 0x0102030405060708L)
    assertEquals(b.getLong(8), 0x0102030405060708L)
    b.putInt(0, 42)
    assertEquals(bb.getInt(0), 42)
    alloc.release(b)
    alloc.trim(force = true)
    assertEquals(alloc.stats.slabs, 0)
  }

  test("a slab's arena is closed when the slab is trimmed") {
    val alloc = BufferAllocator()
    val b = alloc.allocate(32)
    val seg = b.segment
    alloc.release(b)
    alloc.trim(force = true)
    assert(!seg.scope().isAlive)
    intercept[IllegalStateException](seg.get(java.lang.foreign.ValueLayout.JAVA_BYTE, 0L))
  }
}
