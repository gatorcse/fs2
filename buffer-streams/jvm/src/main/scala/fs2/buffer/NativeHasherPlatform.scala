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

import java.security.MessageDigest

import cats.effect.{Resource, Sync}
import fs2.Chunk
import fs2.hashing.{Hash, HashAlgorithm}

private[buffer] trait NativeHasherPlatform {

  private[buffer] def create[F[_]](
      algorithm: HashAlgorithm
  )(implicit F: Sync[F]): Resource[F, NativeHasher[F]] =
    Resource.eval(F.delay {
      val md = MessageDigest.getInstance(NativeHasher.algorithmName(algorithm))
      new NativeHasher[F] {
        // A direct ByteBuffer: the digest reads the native memory through a small
        // reused scratch array, with no per-update allocation.
        protected def unsafeUpdateNative(buf: NativeBuf): Unit = md.update(buf.asByteBuffer)
        protected def unsafeUpdateArray(bytes: Array[Byte], offset: Int, length: Int): Unit =
          md.update(bytes, offset, length)
        protected def unsafeHash(): Hash = Hash(Chunk.array(md.digest()))
      }
    })
}
