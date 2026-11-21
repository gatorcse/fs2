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

import cats.effect.{Resource, Sync}
import fs2.Chunk
import fs2.hashing.{Hash, HashAlgorithm}
import fs2.hashing.openssl._

import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

private[buffer] trait NativeHasherPlatform {

  private[buffer] def create[F[_]](
      algorithm: HashAlgorithm
  )(implicit F: Sync[F]): Resource[F, NativeHasher[F]] = {
    val acquire = F.delay {
      val md = Zone.acquire { implicit z =>
        EVP_get_digestbyname(toCString(NativeHasher.algorithmName(algorithm)))
      }
      if (md == null) throw new IllegalArgumentException(s"unknown digest $algorithm")
      val ctx = EVP_MD_CTX_new()
      if (ctx == null) throw new RuntimeException("EVP_MD_CTX_new failed")
      (md, ctx)
    }
    Resource.make(acquire) { case (_, ctx) => F.delay(EVP_MD_CTX_free(ctx)) }.evalMap {
      case (md, ctx) =>
        F.delay {
          def init(): Unit =
            if (EVP_DigestInit_ex(ctx, md, null) != 1)
              throw new RuntimeException("EVP_DigestInit_ex failed")
          def digest(p: Ptr[Byte], n: Int): Unit =
            if (n > 0 && EVP_DigestUpdate(ctx, p, n.toCSize) != 1)
              throw new RuntimeException("EVP_DigestUpdate failed")
          init()
          new NativeHasher[F] {
            protected def unsafeUpdateNative(buf: NativeBuf): Unit = {
              buf.bulk()
              digest(buf.ptr, buf.size)
            }
            protected def unsafeUpdateArray(bytes: Array[Byte], offset: Int, length: Int): Unit =
              if (length > 0) digest(bytes.atUnsafe(offset), length)
            protected def unsafeHash(): Hash = {
              val out = new Array[Byte](EVP_MAX_MD_SIZE)
              val size = stackalloc[CUnsignedInt]()
              if (EVP_DigestFinal_ex(ctx, out.atUnsafe(0), size) != 1)
                throw new RuntimeException("EVP_DigestFinal_ex failed")
              init()
              Hash(Chunk.array(out, 0, (!size).toInt))
            }
          }
        }
    }
  }
}
