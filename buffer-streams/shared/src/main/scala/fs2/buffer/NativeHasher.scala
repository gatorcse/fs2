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
import fs2.hashing.{HashAlgorithm, Hasher}

/** An fs2 `Hasher` that reads native buffers in place.
  *
  * A drop-in `Hasher`: `update`, `observe`, `drain` and `verify` work as in fs2, and
  * [[NativeChunk]]s are hashed without a heap copy (other chunks are hashed as usual).
  *
  * Uses `MessageDigest` on the JVM and OpenSSL's EVP digests on Scala Native.
  */
abstract class NativeHasher[F[_]] private[buffer] (implicit F: Sync[F]) extends Hasher[F] {

  protected def unsafeUpdateNative(buf: NativeBuf): Unit
  protected def unsafeUpdateArray(bytes: Array[Byte], offset: Int, length: Int): Unit

  protected def unsafeUpdate(chunk: Chunk[Byte]): Unit = chunk match {
    case n: NativeChunk => unsafeUpdateNative(n.buf)
    case _              =>
      val s = chunk.toArraySlice
      unsafeUpdateArray(s.values, s.offset, s.length)
  }

  def update(bytes: Chunk[Byte]): F[Unit] = F.delay(unsafeUpdate(bytes))

  def hash: F[fs2.hashing.Hash] = F.delay(unsafeHash())

  /** Adds the bytes of `buf` to the current hash. */
  def updateNative(buf: NativeBuf): F[Unit] = F.delay(unsafeUpdateNative(buf))
}

object NativeHasher extends NativeHasherPlatform {

  /** A hasher for `algorithm`, released with the resource. */
  def apply[F[_]: Sync](algorithm: HashAlgorithm): Resource[F, NativeHasher[F]] =
    create(algorithm)

  private[buffer] def algorithmName(algorithm: HashAlgorithm): String = algorithm match {
    case HashAlgorithm.MD5         => "MD5"
    case HashAlgorithm.SHA1        => "SHA-1"
    case HashAlgorithm.SHA224      => "SHA-224"
    case HashAlgorithm.SHA256      => "SHA-256"
    case HashAlgorithm.SHA384      => "SHA-384"
    case HashAlgorithm.SHA512      => "SHA-512"
    case HashAlgorithm.SHA512_224  => "SHA-512/224"
    case HashAlgorithm.SHA512_256  => "SHA-512/256"
    case HashAlgorithm.SHA3_224    => "SHA3-224"
    case HashAlgorithm.SHA3_256    => "SHA3-256"
    case HashAlgorithm.SHA3_384    => "SHA3-384"
    case HashAlgorithm.SHA3_512    => "SHA3-512"
    case HashAlgorithm.Named(name) => name
    case other                     => throw new IllegalArgumentException(s"unsupported $other")
  }
}
