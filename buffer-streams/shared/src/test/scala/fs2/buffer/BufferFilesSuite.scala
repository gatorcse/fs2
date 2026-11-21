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

  private def checkedAllocator(checks: Checks = Checks.Full) =
    BufferAllocator(
      BufferAllocator.Config(
        idleTimeout = 1.hour,
        checks = checks,
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

  test("unsafeReadAllNative and writeAll copy files of every size, and drain the pool") {
    sizes.traverse_ { size =>
      val alloc = checkedAllocator()
      val files = BufferFiles.withAllocator[IO](alloc)
      (source(size), Files[IO].tempFile).tupled.use { case ((src, bytes), dst) =>
        files.unsafeReadAllNative(src, chunk).through(files.writeAll(dst)).compile.drain >>
          readBack(dst).map(assertEquals(_, bytes))
      } >> assertDrained(alloc)
    }
  }

  test("readAll emits heap chunks, copies files of every size, and holds no native memory") {
    sizes.traverse_ { size =>
      val alloc = checkedAllocator()
      val files = BufferFiles.withAllocator[IO](alloc)
      (source(size), Files[IO].tempFile).tupled.use { case ((src, bytes), dst) =>
        files
          .readAll(src, chunk)
          .chunks
          .evalTap(c => IO(assert(!c.isInstanceOf[NativeChunk], c.getClass.getName)))
          .unchunks
          .through(files.writeAll(dst))
          .compile
          .drain >> readBack(dst).map(assertEquals(_, bytes))
      } >> assertDrained(alloc)
    }
  }

  test("readAll's chunks are safe wherever they go, with no lifetime rules") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    source(10 * chunk + 1).use { case (src, bytes) =>
      for {
        held <- files.readAll(src, chunk).chunks.compile.toList
        viaPar <- files
          .readAll(src, chunk)
          .chunks
          .parEvalMap(3)(c => IO.sleep(2.millis).as(c))
          .unchunks
          .compile
          .to(Chunk)
        tail <- files.readAll(src, chunk).chunkN(3 * chunk).compile.toList
      } yield {
        assertEquals(Chunk.concat(held), bytes)
        assertEquals(viaPar, bytes)
        assertEquals(Chunk.concat(tail), bytes)
      }
    }
  }

  test("unsafeReadAllNative handles chunks larger than the pooled sizes") {
    val alloc = checkedAllocator()
    val files = BufferFiles.withAllocator[IO](alloc)
    (source(3 * 1024 * 1024 + 7), Files[IO].tempFile).tupled.use { case ((src, bytes), dst) =>
      files.unsafeReadAllNative(src, 1024 * 1024).through(files.writeAll(dst)).compile.drain >>
        readBack(dst).map(assertEquals(_, bytes))
    } >> assertDrained(alloc)
  }

  test("charset decoding keeps no view of native memory past the source") {
    // decodeWithCharset can hold part of a chunk as a ByteBuffer and decode it after the
    // source ends; toByteBuffer copies, so that part stays correct.
    assume(java.nio.charset.Charset.isSupported("Shift_JIS"), "needs Shift_JIS")
    val cs = java.nio.charset.Charset.forName("Shift_JIS")
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    val text = ("native chunks " * 75).take(1050)
    Files[IO].tempFile.use { src =>
      Stream.chunk(Chunk.array(text.getBytes(cs))).through(Files[IO].writeAll(src)).compile.drain >>
        files
          .unsafeReadAllNative(src, 4096)
          .through(fs2.text.decodeWithCharset(cs))
          .compile
          .string
          .map(assertEquals(_, text))
    }
  }

  test("native chunks are emitted, and sliced without copying") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    source(3 * chunk).use { case (src, bytes) =>
      files
        .unsafeReadAllNative(src, chunk)
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

  test("writeAll appends, and writes heap chunks larger than a buffer") {
    val files = BufferFiles[IO]
    val big = Chunk.array(Array.tabulate(300000)(i => (i * 7).toByte))
    Files[IO].tempFile.use { dst =>
      Stream.chunk(big).through(files.writeAll(dst)).compile.drain >>
        Stream.chunk(big).through(files.writeAll(dst, append = true)).compile.drain >>
        readBack(dst).map(assertEquals(_, big ++ big))
    }
  }

  test("native chunks flow into fs2-io's own file writer") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    (source(5 * chunk + 3), Files[IO].tempFile).tupled.use { case ((src, bytes), dst) =>
      files.unsafeReadAllNative(src, chunk).through(Files[IO].writeAll(dst)).compile.drain >>
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
            files.unsafeReadAllNative(src, chunk).through(h.observe(_.drain)).compile.lastOrError
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

  test("sendFile appends, and copies an empty file") {
    val files = BufferFiles[IO]
    (source(3 * chunk + 9), source(0), Files[IO].tempFile).tupled.use {
      case ((src, bytes), (empty, _), dst) =>
        for {
          _ <- files.sendFile(src, dst)
          n <- files.sendFile(src, dst, append = true)
          m <- files.sendFile(empty, dst, append = true)
          out <- readBack(dst)
        } yield {
          assertEquals(n, bytes.size.toLong)
          assertEquals(m, 0L)
          assertEquals(out, bytes ++ bytes)
        }
    }
  }

  test("sendFile refuses to copy a file onto itself") {
    val files = BufferFiles[IO]
    source(2 * chunk).use { case (src, bytes) =>
      files.sendFile(src, src).intercept[java.io.IOException] >>
        readBack(src).map(assertEquals(_, bytes))
    }
  }

  test("a missing file fails with NoSuchFileException, as fs2-io does") {
    Files[IO].tempDirectory.use { dir =>
      BufferFiles[IO]
        .readAll(dir / "missing")
        .compile
        .drain
        .intercept[java.nio.file.NoSuchFileException]
        .void
    }
  }

  test("chunks copied to the heap first are safe in concurrent stages") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    (source(10 * chunk + 1), source(7 * chunk)).tupled.use { case ((a, ab), (b, bb)) =>
      for {
        viaPar <- files
          .unsafeReadAllNative(a, chunk)
          .through(NativeChunk.heap)
          .chunks
          .parEvalMap(3)(c => IO.sleep(2.millis).as(c))
          .unchunks
          .compile
          .to(Chunk)
        viaJoin <- Stream(a, b)
          .map(p =>
            files
              .unsafeReadAllNative(p, chunk)
              .through(NativeChunk.heap)
              .chunks
              .fold(Chunk.empty[Byte])(_ ++ _)
          )
          .parJoin(2)
          .evalTap(_ => IO.sleep(2.millis))
          .compile
          .toList
      } yield {
        assertEquals(viaPar, ab)
        assertEquals(viaJoin.map(_.size).sorted, List(bb.size, ab.size).sorted)
      }
    }
  }

  test("toByteVector copies, so it stays correct after the window moves on") {
    // At the default Bulk level element reads are unchecked, so a lazy view would silently
    // read reused memory here.
    val files = BufferFiles.withAllocator[IO](checkedAllocator(Checks.Bulk))
    source(10 * chunk + 3).use { case (src, bytes) =>
      for {
        bv <- files.unsafeReadAllNative(src, chunk).compile.to(scodec.bits.ByteVector)
        bits <- files.unsafeReadAllNative(src, chunk).chunks.map(_.toBitVector).compile.toList
      } yield {
        assertEquals(bv, bytes.toByteVector)
        assertEquals(bits.reduce(_ ++ _), bytes.toBitVector)
      }
    }
  }

  test("unsafeReadAllNative frees its file and buffers at end of input, even inside flatMap") {
    val fdDir = java.nio.file.Paths.get("/proc/self/fd")
    def openFds = IO.blocking(java.nio.file.Files.list(fdDir).count())
    val alloc = checkedAllocator()
    val files = BufferFiles.withAllocator[IO](alloc)
    IO.blocking(java.nio.file.Files.isDirectory(fdDir)).flatMap { linux =>
      assume(linux, "needs /proc/self/fd")
      source(chunk + 1).use { case (src, _) =>
        for {
          before <- openFds
          during <- IO.ref((0L, 0))
          // Counted straight after the reads, in the same scope: each file and window must
          // already be released, not just when that scope ends.
          _ <- (Stream.range(0, 50).flatMap(_ => files.unsafeReadAllNative(src, chunk)) ++
            Stream.exec(openFds.flatMap(n => during.set((n, alloc.stats.magazines))))).compile.drain
          after <- during.get
        } yield {
          assert(after._1 < before + 10, s"$before fds before, ${after._1} during")
          assertEquals(after._2, 0, "magazines still open")
        }
      }
    }
  }

  test("a stage holding chunks within the window works") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    source(9 * chunk).use { case (src, bytes) =>
      // 3 chunks of 4: within the default window of 4. Every group fills before the file
      // ends, so none is used after the source's buffers are freed.
      files
        .unsafeReadAllNative(src, chunk)
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
        .unsafeReadAllNative(src, chunk)
        .chunkN(6 * chunk)
        .map(_.toArray)
        .compile
        .drain
        .intercept[IllegalStateException]
        .void
    }
  }

  test("misuse: a stage emitting after the source ends sees released chunks") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    source(10 * chunk).use { case (src, _) =>
      // chunkN's last, partial group is emitted after the source has ended.
      files
        .unsafeReadAllNative(src, chunk)
        .chunkN(3 * chunk)
        .map(_.toArray)
        .compile
        .drain
        .intercept[IllegalStateException]
        .void
    }
  }

  test("misuse: broadcastThrough hands chunks to other fibers") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    source(10 * chunk).use { case (src, _) =>
      files
        .unsafeReadAllNative(src, chunk)
        .broadcastThrough(
          (s: Stream[IO, Byte]) => s.chunks.evalMap(c => IO.sleep(1.milli).as(c.toArray)).drain,
          (s: Stream[IO, Byte]) => s.drain
        )
        .compile
        .drain
        .intercept[IllegalStateException]
        .void
    }
  }

  test("element traversals of a stale chunk fail at the default checks level") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator(Checks.Bulk))
    source(3 * chunk).use { case (src, _) =>
      files.unsafeReadAllNative(src, chunk).chunks.compile.toList.map { chunks =>
        val c = chunks.head
        intercept[IllegalStateException](c.iterator.size)
        intercept[IllegalStateException](c.foreach(_ => ()))
        intercept[IllegalStateException](c.map(_ + 1))
        intercept[IllegalStateException](c.toByteBuffer)
        ()
      }
    }
  }

  test("misuse: keeping native chunks past the stream fails under checks") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    source(3 * chunk).use { case (src, _) =>
      for {
        chunks <- files.unsafeReadAllNative(src, chunk).chunks.compile.toList
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
          .unsafeReadAllNative(src, chunk)
          .through(NativeChunk.heap)
          .chunkN(6 * chunk)
          .compile
          .foldMonoid
      } yield assertEquals(Chunk.array(viaChunks.toArray), bytes)
    }
  }

  test("NativeChunk.heap copies re-chunked native data too") {
    val files = BufferFiles.withAllocator[IO](checkedAllocator())
    // A whole number of groups, so buffer emits each one before the source ends; a partial
    // last group would come after it, which is why heap belongs before re-chunking.
    source(10 * chunk).use { case (src, bytes) =>
      files
        .unsafeReadAllNative(src, chunk)
        .buffer(2 * chunk)
        .through(NativeChunk.heap)
        .chunks
        .compile
        .toList
        .map(cs => assertEquals(Chunk.concat(cs.map(c => Chunk.array(c.toArray))), bytes))
    }
  }

  test("cancelling a read part way releases every buffer") {
    val alloc = checkedAllocator()
    val files = BufferFiles.withAllocator[IO](alloc)
    source(50 * chunk).use { case (src, _) =>
      List(0, 1, 2, 5).traverse_ { n =>
        files.unsafeReadAllNative(src, chunk).chunks.take(n.toLong).compile.drain >>
          files
            .unsafeReadAllNative(src, chunk)
            .evalTap(_ => IO.cede)
            .compile
            .drain
            .timeoutTo(1.milli, IO.unit)
      }
    } >> assertDrained(alloc)
  }
}
