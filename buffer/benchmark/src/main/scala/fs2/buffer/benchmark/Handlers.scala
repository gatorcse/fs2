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

import cats.effect.IO
import fs2.hashing.{Hash, HashAlgorithm, Hashing}
import fs2.io.file.{Files, Path}

/** The same upload and download handlers on today's heap path and on native chunks.
  * Sockets are not on native buffers yet (milestone D), so a file stands in for the
  * request body and a file or `/dev/null` for the response socket.
  *
  *   - upload: stream the body to disk and return its SHA-256, as an upload endpoint would;
  *   - download: stream a file to the client.
  */
object Handlers {

  // ---- today: heap chunks through fs2-io and fs2's hasher ----

  object heap {
    def upload(body: Path, dest: Path): IO[Hash] =
      Hashing[IO].hasher(HashAlgorithm.SHA256).use { h =>
        Files[IO].readAll(body).through(h.observe(Files[IO].writeAll(dest))).compile.lastOrError
      }

    def download(file: Path, client: Path): IO[Unit] =
      Files[IO].readAll(file).through(Files[IO].writeAll(client)).compile.drain
  }

  // ---- native chunks: an ordinary Stream[F, Byte] ----

  object chunk {
    private val files = BufferFiles[IO]

    def upload(body: Path, dest: Path): IO[Hash] =
      NativeHasher[IO](HashAlgorithm.SHA256).use { h =>
        files.readAll(body).through(h.observe(files.writeAll(dest))).compile.lastOrError
      }

    def download(file: Path, client: Path): IO[Unit] =
      files.readAll(file).through(files.writeAll(client)).compile.drain
  }

  // ---- no buffer at all ----

  object sendfile {
    def download(file: Path, client: Path): IO[Unit] =
      BufferFiles[IO].sendFile(file, client).void
  }
}
