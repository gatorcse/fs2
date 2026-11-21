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

import java.lang.management.ManagementFactory

import cats.effect.{ExitCode, IO, IOApp}
import cats.syntax.all._
import fs2.Stream
import fs2.io.file.{Files, Path}

import scala.jdk.CollectionConverters._

/** Milestone C's gate: a 1 GB upload and download per variant, measuring time, heap
  * allocation, GC, heap growth, native pool size and peak RSS. Run each variant in its own
  * JVM so peak RSS is its own, for example:
  *
  * {{{
  * bufferBenchmark/runMain fs2.buffer.benchmark.FileStreamsBenchmark upload chunk 1024 /tmp/bench
  * }}}
  *
  * Arguments: `upload|download`, `heap|chunk|leased|sendfile`, size in MiB, work directory.
  */
object FileStreamsBenchmark extends IOApp {

  private val runs = 3

  def run(args: List[String]): IO[ExitCode] = args match {
    case List(op, variant, mib, dir) =>
      val work = Path(dir)
      val src = work / s"source-$mib.bin"
      val dst = work / s"dest-$variant.bin"
      val size = mib.toLong * 1024 * 1024
      val step: IO[Unit] = (op, variant) match {
        case ("upload", "heap")       => Handlers.heap.upload(src, dst).void
        case ("upload", "chunk")      => Handlers.chunk.upload(src, dst).void
        case ("upload", "leased")     => Handlers.leased.upload(src, dst).void
        case ("download", "heap")     => Handlers.heap.download(src, Path("/dev/null"))
        case ("download", "chunk")    => Handlers.chunk.download(src, Path("/dev/null"))
        case ("download", "leased")   => Handlers.leased.download(src, Path("/dev/null"))
        case ("download", "sendfile") => Handlers.sendfile.download(src, Path("/dev/null"))
        case _ => IO.raiseError(new IllegalArgumentException(s"$op $variant"))
      }
      for {
        _ <- Files[IO].createDirectories(work)
        _ <- ensureSource(src, size)
        _ <- step // warm-up
        before <- snapshot
        times <- step.timed.map(_._1.toMillis).replicateA(runs)
        after <- snapshot
        _ <- Files[IO].deleteIfExists(dst)
        _ <- IO.println(report(op, variant, size, times, before, after))
      } yield ExitCode.Success
    case _ =>
      IO.println("usage: upload|download heap|chunk|leased|sendfile <MiB> <dir>").as(ExitCode.Error)
  }

  /** Writes `size` bytes of a repeating pattern, once, outside the measurement. */
  private def ensureSource(src: Path, size: Long): IO[Unit] =
    Files[IO].exists(src).flatMap { exists =>
      Files[IO].size(src).map(_ == size).recover(_ => false).flatMap { ok =>
        if (exists && ok) IO.unit
        else {
          val block = fs2.Chunk.array(Array.tabulate(1 << 20)(i => (i * 131 + (i >> 9)).toByte))
          Stream.chunk(block).repeat.take(size).through(Files[IO].writeAll(src)).compile.drain
        }
      }
    }

  private final case class Snapshot(
      allocated: Long,
      gcCount: Long,
      gcMillis: Long,
      heapUsed: Long,
      peakRss: Long,
      slabBytes: Long
  )

  private def snapshot: IO[Snapshot] = IO.blocking {
    System.gc()
    val threads = ManagementFactory.getThreadMXBean.asInstanceOf[com.sun.management.ThreadMXBean]
    val gcs = ManagementFactory.getGarbageCollectorMXBeans.asScala
    Snapshot(
      allocated = threads.getTotalThreadAllocatedBytes,
      gcCount = gcs.map(_.getCollectionCount).sum,
      gcMillis = gcs.map(_.getCollectionTime).sum,
      heapUsed = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage.getUsed,
      peakRss = peakRssKiB * 1024,
      slabBytes = BufferAllocator.global.stats.slabBytes
    )
  }

  private def peakRssKiB: Long =
    scala.util
      .Try {
        val src = scala.io.Source.fromFile("/proc/self/status")
        try
          src.getLines().collectFirst { case l if l.startsWith("VmHWM:") => l }.get
        finally src.close()
      }
      .map(_.split("\\s+")(1).toLong)
      .getOrElse(-1L)

  private def report(
      op: String,
      variant: String,
      size: Long,
      times: List[Long],
      before: Snapshot,
      after: Snapshot
  ): String = {
    val mib = 1024.0 * 1024
    val best = times.min
    val perRun = (after.allocated - before.allocated).toDouble / runs
    f"""RESULT op=$op variant=$variant size=${size / mib}%.0fMiB
       |  time: best ${best}ms, runs ${times.mkString(
        ","
      )}ms, ${size / mib / (best / 1000.0)}%.0f MiB/s
       |  heap allocated per run: ${perRun / mib}%.1f MiB (${perRun / (size / 65536.0)}%.0f B per 64 KiB)
       |  GCs during runs: ${after.gcCount - before.gcCount} (${after.gcMillis - before.gcMillis} ms)
       |  heap used after GC: before ${before.heapUsed / mib}%.1f MiB, after ${after.heapUsed / mib}%.1f MiB
       |  native pool: ${after.slabBytes / mib}%.1f MiB of slabs
       |  peak RSS: ${after.peakRss / mib}%.0f MiB""".stripMargin
  }
}
