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

import java.lang.foreign.{Arena, FunctionDescriptor, Linker, MemorySegment, ValueLayout}
import java.lang.invoke.MethodHandle
import java.util.concurrent.TimeUnit

import org.openjdk.jmh.annotations._
import org.openjdk.jmh.infra.Blackhole

/** Cost of one allocate + release of a TLS-record-sized buffer, for each option in the
  * plan's allocator table. Run with, for example:
  *
  * {{{
  * bufferBenchmark/Jmh/run -i 5 -wi 3 -f 1 -t 1 .*AllocatorBenchmark.*
  * }}}
  *
  * Use `-t 4` or more to see how the shared-arena close handshake scales with threads.
  */
@State(Scope.Thread)
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.NANOSECONDS)
@Fork(value = 1, jvmArgsAppend = Array("--enable-native-access=ALL-UNNAMED"))
class AllocatorBenchmark {

  @Param(Array("16640"))
  var size: Int = _

  private var magazine: Magazine = _

  @Setup(Level.Trial)
  def setup(): Unit = magazine = AllocatorBenchmark.allocator.newMagazine()

  @TearDown(Level.Trial)
  def tearDown(): Unit = magazine.close()

  /** Today's path: a fresh heap array per read. */
  @Benchmark
  def heapArray(bh: Blackhole): Unit = bh.consume(new Array[Byte](size))

  /** Magazine fast path: no atomics, no locks. */
  @Benchmark
  def pooledMagazine(bh: Blackhole): Unit = {
    val b = magazine.allocate(size)
    bh.consume(b)
    magazine.release(b)
  }

  /** Allocated from the locked shared pool, released through the lock-free remote list. */
  @Benchmark
  def pooledShared(bh: Blackhole): Unit = {
    val b = AllocatorBenchmark.allocator.allocate(size)
    bh.consume(b)
    AllocatorBenchmark.allocator.release(b)
  }

  @Benchmark
  def confinedArenaPerBuffer(bh: Blackhole): Unit = {
    val arena = Arena.ofConfined()
    bh.consume(arena.allocate(size.toLong, 64L))
    arena.close()
  }

  @Benchmark
  def sharedArenaPerBuffer(bh: Blackhole): Unit = {
    val arena = Arena.ofShared()
    bh.consume(arena.allocate(size.toLong, 64L))
    arena.close()
  }

  /** Freed later by the GC through a Cleaner. */
  @Benchmark
  def autoArena(bh: Blackhole): Unit =
    bh.consume(Arena.ofAuto().allocate(size.toLong, 64L))

  /** `malloc`/`free` downcalls plus `reinterpret`. Needs `--enable-native-access`. */
  @Benchmark
  def mallocDowncall(bh: Blackhole): Unit = {
    val p = AllocatorBenchmark.malloc.invoke(size.toLong).asInstanceOf[MemorySegment]
    bh.consume(p.reinterpret(size.toLong))
    AllocatorBenchmark.free.invoke(p): Unit
  }
}

object AllocatorBenchmark {
  val allocator: BufferAllocator = BufferAllocator()

  private val linker = Linker.nativeLinker()
  val malloc: MethodHandle = linker.downcallHandle(
    linker.defaultLookup().find("malloc").get(),
    FunctionDescriptor.of(ValueLayout.ADDRESS, ValueLayout.JAVA_LONG)
  )
  val free: MethodHandle = linker.downcallHandle(
    linker.defaultLookup().find("free").get(),
    FunctionDescriptor.ofVoid(ValueLayout.ADDRESS)
  )
}
