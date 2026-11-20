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

import java.util.concurrent.atomic.AtomicInteger

import cats.effect.IO
import cats.effect.testkit.TestControl
import cats.syntax.all._

import scala.concurrent.duration._
import scala.util.Random

/** The milestone B gate: lease-based programs cancelled at every step leave no lease
  * outstanding.
  *
  * Random programs nest leases with sleeps, cedes, errors, parallel and racing branches
  * and uncancelable regions. Each one runs under `TestControl` and is cancelled after
  * every virtual millisecond of its run, and after 0 to 5 extra cedes at each of those
  * instants. After every run the allocator must drain completely: no slab, page or
  * unpooled buffer left. Checks are at [[Checks.Full]] with poisoning on, so a lease used
  * after its release fails the run instead of passing silently.
  */
class CancellationSuite extends munit.CatsEffectSuite {
  import CancellationSuite._

  override def munitIOTimeout: Duration = 10.minutes

  private def newAllocator() =
    BufferAllocator(
      BufferAllocator.Config(
        slabSize = 2 * 1024 * 1024,
        idleTimeout = 1.hour,
        checks = Checks.Full,
        poison = true,
        leakDetection = LeakDetection.Off
      )
    )

  private val sizes = Vector(16, 3000, 20000, 70000, 300000)

  private def gen(rnd: Random, depth: Int): Prog =
    if (depth <= 0) leaf(rnd)
    else
      rnd.nextInt(10) match {
        case 0 | 1 | 2 => Lease(sizes(rnd.nextInt(sizes.length)), gen(rnd, depth - 1))
        case 3 | 4     => Then(gen(rnd, depth - 1), gen(rnd, depth - 1))
        case 5         => Both(gen(rnd, depth - 1), gen(rnd, depth - 1))
        case 6         => Race(gen(rnd, depth - 1), gen(rnd, depth - 1))
        case 7         => Uncancelable(gen(rnd, depth - 1))
        case _         => leaf(rnd)
      }

  private def leaf(rnd: Random): Prog = rnd.nextInt(5) match {
    case 0 => Fail
    case 1 => Cede
    case _ => Sleep(rnd.nextInt(3))
  }

  private def interpret(p: Prog, alloc: BufferAllocator, tags: AtomicInteger): IO[Unit] =
    p match {
      case Lease(size, body) =>
        alloc.lease[IO](size).use { b =>
          val tag = tags.incrementAndGet()
          IO(b.putInt(0, tag)) *> interpret(body, alloc, tags) *> IO {
            if (b.getInt(0) != tag) throw new AssertionError(s"$b changed while leased")
          }
        }
      case Then(a, b)      => interpret(a, alloc, tags) *> interpret(b, alloc, tags)
      case Both(a, b)      => (interpret(a, alloc, tags), interpret(b, alloc, tags)).parTupled.void
      case Race(a, b)      => IO.race(interpret(a, alloc, tags), interpret(b, alloc, tags)).void
      case Uncancelable(a) => IO.uncancelable(_ => interpret(a, alloc, tags))
      case Sleep(ms)       => IO.sleep(ms.millis)
      case Cede            => IO.cede
      case Fail            => IO.raiseError(Boom)
    }

  /** Runs `p` and returns any non-`Boom` failure, so an AssertionError inside the
    * program fails the test even when cancellation hides it.
    */
  private def runOnce(p: Prog, cancelAfter: Option[(Int, Int)]): IO[Unit] = {
    val alloc = newAllocator()
    val tags = new AtomicInteger
    val prog = interpret(p, alloc, tags).attempt.flatMap {
      case Left(Boom) | Right(_) => IO.unit
      case Left(e)               => IO.raiseError(e)
    }
    val test = cancelAfter match {
      case None              => prog
      case Some((ms, cedes)) =>
        prog.start.flatMap { f =>
          IO.sleep(ms.millis) *> IO.cede.replicateA_(cedes) *> f.cancel *> f.join.flatMap(
            _.fold(IO.unit, IO.raiseError, identity)
          )
        }
    }
    TestControl.executeEmbed(test) *> IO {
      alloc.trim(force = true)
      val s = alloc.stats
      assertEquals(s.slabs, 0, s"$p cancelled at $cancelAfter: $s")
      assertEquals(s.unpooledBuffers, 0L, s"$p cancelled at $cancelAfter: $s")
    }
  }

  private def duration(p: Prog): IO[Int] =
    TestControl.executeEmbed(interpret(p, newAllocator(), new AtomicInteger).attempt.timed).map {
      case (d, _) => d.toMillis.toInt
    }

  test("cancelling lease programs at every step leaves no lease outstanding") {
    val seeds = 0 until 600
    val runs = new AtomicInteger
    val leases = new AtomicInteger
    seeds.toList.traverse_ { seed =>
      val p = gen(new Random(seed.toLong), 5)
      for {
        _ <- runOnce(p, None)
        total <- duration(p)
        _ <- (0 to total).toList.traverse_ { ms =>
          (0 to 5).toList.traverse_(cedes =>
            runOnce(p, Some((ms, cedes))) *> IO(runs.incrementAndGet())
          )
        }
        _ <- IO(leases.addAndGet(countLeases(p)))
      } yield ()
    } *> IO {
      // Guard against the fuzzer quietly testing nothing.
      println(s"cancellation fuzz: ${runs.get()} cancelled runs over ${leases.get()} leases")
      assert(runs.get() > 5000, s"only ${runs.get()} cancelled runs")
      assert(leases.get() > 500, s"only ${leases.get()} leases in the programs")
    }
  }

  private def countLeases(p: Prog): Int = p match {
    case Lease(_, b)     => 1 + countLeases(b)
    case Then(a, b)      => countLeases(a) + countLeases(b)
    case Both(a, b)      => countLeases(a) + countLeases(b)
    case Race(a, b)      => countLeases(a) + countLeases(b)
    case Uncancelable(a) => countLeases(a)
    case _               => 0
  }

  test("a lease is released on error and on cancellation of an uncancelable-acquired use") {
    val alloc = newAllocator()
    for {
      _ <- alloc.lease[IO](100).use(_ => IO.raiseError[Unit](Boom)).attempt
      fiber <- alloc.lease[IO](100).use(_ => IO.never[Unit]).start
      _ <- IO.cede.replicateA_(3)
      _ <- fiber.cancel
      _ <- IO {
        alloc.trim(force = true)
        assertEquals(alloc.stats.slabs, 0)
      }
    } yield ()
  }
}

object CancellationSuite {
  sealed trait Prog
  final case class Lease(size: Int, body: Prog) extends Prog
  final case class Then(a: Prog, b: Prog) extends Prog
  final case class Both(a: Prog, b: Prog) extends Prog
  final case class Race(a: Prog, b: Prog) extends Prog
  final case class Uncancelable(a: Prog) extends Prog
  final case class Sleep(ms: Int) extends Prog
  case object Cede extends Prog
  case object Fail extends Prog

  case object Boom extends RuntimeException("boom")
}
