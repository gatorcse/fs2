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

import cats.effect.{IO, Resource}
import cats.syntax.all._
import fs2.{Chunk, Stream}
import fs2.hashing.{HashAlgorithm, Hashing}
import fs2.io.file.{Files, Path}

import scala.concurrent.duration._

class BufferFilesSuite extends munit.CatsEffectSuite {

  private val chunk = 4096

  private def checkedAllocator() =
    BufferAllocator(
      BufferAllocator.Config(
        idleTimeout = 1.hour,
        checks = Checks.Full,
        poison = true,
        leakDetection = LeakDetection.Off
      )
    )

  /** A temp file holding `size` pseudo-random bytes, and those bytes. */
  private def source(size: Int): Resource[IO, (Path, Chunk[Byte])] =
    Files[IO].tempFile.evalMap { p =>
      val bytes = Chunk.array(Array.tabulate(size)(i => ((i * 31 + i / 7) & 0xff).toByte))
      Stream.chunk(bytes).through(Files[IO].writeAll(p)).compile.drain.as(p -> bytes)
    }

  private def assertDrained(alloc: BufferAllocator): IO[Unit] = IO {
    alloc.trim(force = true)
    val s = alloc.stats
    assertEquals(s.slabs, 0, s.toString)
    assertEquals(s.unpooledBuffers, 0L, s.toString)
  }

  private def readBack(p: Path): IO[Chunk[Byte]] = Files[IO].readAll(p).chunks.compile.foldMonoid

  private val sizes = List(0, 1, chunk - 1, chunk, chunk + 1, 3 * chunk + 5)

  test("readAll and writeAll copy files of every size, and drain the pool") {
    sizes.traverse_ { size =>
      val alloc = checkedAllocator()
      val files = BufferFiles.withAllocator[IO](alloc)
      (source(size), Files[IO].tempFile).tupled.use { case ((src, bytes), dst) =>
        files.readAll(src, chunk).through(files.writeAll(dst)).compile.drain >>
          readBack(dst).map(assertEquals(_, bytes))
      } >> assertDrained(alloc)
    }
  }

  test("native chunks are emitted, and sliced without copying") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    source(3 * chunk).use { case (src, bytes) =>
      files
        .readAll(src, chunk)
        .chunks
        .map { c =>
          assert(c.isInstanceOf[NativeChunk], c.getClass.getName)
          val (a, b) = c.splitAt(100)
          assert(a.isInstanceOf[NativeChunk] && b.isInstanceOf[NativeChunk])
          Chunk.array((a ++ b).toArray)
        }
        .compile
        .foldMonoid
        .map(assertEquals(_, bytes))
    }
  }

  test("writeAll accepts heap chunks too") {
    val alloc = checkedAllocator()
    val files = BufferFiles.withAllocator[IO](alloc)
    val bytes = Chunk.array(Array.tabulate(200000)(_.toByte))
    Files[IO].tempFile.use { dst =>
      Stream.chunk(bytes).through(files.writeAll(dst)).compile.drain >>
        readBack(dst).map(assertEquals(_, bytes))
    } >> assertDrained(alloc)
  }

  test("native chunks flow into fs2-io's own file writer") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    (source(5 * chunk + 3), Files[IO].tempFile).tupled.use { case ((src, bytes), dst) =>
      files.readAll(src, chunk).through(Files[IO].writeAll(dst)).compile.drain >>
        readBack(dst).map(assertEquals(_, bytes))
    }
  }

  test("NativeHasher agrees with fs2's hasher") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    val algorithms = List(HashAlgorithm.MD5, HashAlgorithm.SHA1, HashAlgorithm.SHA256)
    source(3 * chunk + 17).use { case (src, bytes) =>
      algorithms.traverse_ { alg =>
        for {
          expected <- Stream.chunk(bytes).through(Hashing[IO].hash(alg)).compile.lastOrError
          viaChunks <- NativeHasher[IO](alg).use { h =>
            files.readAll(src, chunk).through(h.observe(_.drain)).compile.lastOrError
          }
          viaHeap <- NativeHasher[IO](alg).use { h =>
            Stream.chunk(bytes).through(h.drain).compile.lastOrError
          }
        } yield {
          assertEquals(viaChunks, expected, alg.toString)
          assertEquals(viaHeap, expected, alg.toString)
        }
      }
    }
  }

  test("sendFile copies a file inside the kernel") {
    val files = BufferFiles[IO]
    (source(3 * chunk + 9), Files[IO].tempFile).tupled.use { case ((src, bytes), dst) =>
      files.sendFile(src, dst).flatMap { n =>
        assertEquals(n, bytes.size.toLong)
        readBack(dst).map(assertEquals(_, bytes))
      }
    }
  }

  test("a stage holding chunks within the window works") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    source(10 * chunk).use { case (src, bytes) =>
      // 3 chunks of 4: within the default window of 4.
      files
        .readAll(src, chunk)
        .chunkN(3 * chunk)
        .map(c => Chunk.array(c.toArray))
        .compile
        .foldMonoid
        .map(assertEquals(_, bytes))
    }
  }

  test("misuse: a stage holding more chunks than the window fails under checks") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    source(10 * chunk).use { case (src, _) =>
      files
        .readAll(src, chunk)
        .chunkN(6 * chunk)
        .map(_.toArray)
        .compile
        .drain
        .intercept[IllegalStateException]
        .void
    }
  }

  test(
    "misuse: behind a scope boundary, a stage emitting after the source ends sees released chunks"
  ) {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    source(10 * chunk).use { case (src, _) =>
      // `chunks` opens a scope that closes when the source ends, before `chunkN` emits its
      // final partial group, so that group is already released. Without the boundary the
      // same pipeline works (see "a stage holding chunks within the window works").
      files
        .readAll(src, chunk)
        .chunks
        .unchunks
        .chunkN(3 * chunk)
        .map(_.toArray)
        .compile
        .drain
        .intercept[IllegalStateException]
        .void
    }
  }

  test("misuse: keeping native chunks past the stream fails under checks") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    source(3 * chunk).use { case (src, _) =>
      for {
        chunks <- files.readAll(src, chunk).chunks.compile.toList
      } yield {
        assert(chunks.forall(_.asInstanceOf[NativeChunk].isReleased))
        intercept[IllegalStateException](chunks.head.toArray)
        ()
      }
    }
  }

  test("copying to the heap first makes long-held chunks safe") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    source(10 * chunk).use { case (src, bytes) =>
      for {
        viaChunks <- files
          .readAll(src, chunk)
          .through(NativeChunk.heap)
          .chunkN(6 * chunk)
          .compile
          .foldMonoid
      } yield assertEquals(Chunk.array(viaChunks.toArray), bytes)
    }
  }

  test("cancelling a read part way releases every buffer") {
    val alloc = checkedAllocator()
    val files = BufferFiles.withAllocator[IO](alloc)
    source(50 * chunk).use { case (src, _) =>
      List(0, 1, 2, 5).traverse_ { n =>
        files.readAll(src, chunk).chunks.take(n.toLong).compile.drain >>
          files
            .readAll(src, chunk)
            .evalTap(_ => IO.cede)
            .compile
            .drain
            .timeoutTo(1.milli, IO.unit)
      }
    } >> assertDrained(alloc)
  }
}
