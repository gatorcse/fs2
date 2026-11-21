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
package benchmark

import java.nio.{ByteBuffer, ByteOrder}
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._

/** Cost of `NativeBuf`'s primitive accessors: big-endian int reads, across a 16 KiB
  * buffer and one at a time, against a direct `ByteBuffer`. Run with
  *
  * {{{
  * bufferBenchmark/Jmh/run -i 5 -wi 3 -f 1 .*AccessorBenchmark.*
  * }}}
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
class AccessorBenchmark {

  private[this] val size = 16384
  private[this] var buf: NativeBuf = _
  private[this] var bb: ByteBuffer = _

  @Setup
  def setup(): Unit = {
    buf = AccessorBenchmark.allocator.allocate(size)
    var i = 0
    while (i < size) { buf.putByte(i, i.toByte); i += 1 }
    bb = ByteBuffer.allocateDirect(size).order(ByteOrder.BIG_ENDIAN)
  }

  @TearDown
  def tearDown(): Unit = AccessorBenchmark.allocator.release(buf)

  @Benchmark
  def nativeBufGetInt(): Int = {
    val b = buf
    var sum = 0
    var i = 0
    while (i < size) { sum += b.getInt(i); i += 4 }
    sum
  }

  private[this] val idx = 100

  @Benchmark
  def nativeBufSingle(): Int = buf.getInt(idx)

  @Benchmark
  def directByteBuffer(): Int = {
    val b = bb
    var sum = 0
    var i = 0
    while (i < size) { sum += b.getInt(i); i += 4 }
    sum
  }
}

object AccessorBenchmark {
  val allocator: BufferAllocator = BufferAllocator()
}
