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

package fs2.io.internal

import org.typelevel.scalaccompat.annotation._

import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix.errno.errno
import scala.scalanative.posix.{fcntl => pfcntl}
import scala.scalanative.posix.string.{strerror => cstrerror}
import scala.scalanative.unsafe._
import scala.scalanative.unsigned._

/** [[SocketSyscalls]] for Scala Native, with `@extern` calls. The GC doesn't move objects, so
  * heap arrays are passed to the kernel directly.
  */
private[fs2] object NativeSocketSyscalls extends SocketSyscalls {
  val constants: SocketConstants =
    if (LinktimeInfo.isMac) SocketConstants.Darwin else SocketConstants.Linux

  /** The shared socket logic over these syscalls. */
  lazy val sockets: FdSockets = new FdSockets(this)

  @inline private[this] def ret(r: CInt): Int = if (r < 0) -errno else r
  @inline private[this] def retS(r: CSSize): Int = if (r < 0) -errno else r.toInt

  @inline private[this] def ptr(a: Array[Byte], offset: Int): Ptr[Byte] =
    if (a eq null) null else a.atUnsafe(offset)

  @inline private[this] def lenPtr(a: Array[Int]): Ptr[UInt] =
    a.atUnsafe(0).asInstanceOf[Ptr[UInt]]

  def socket(domain: Int, tpe: Int, protocol: Int): Int = ret(
    nativesocket.socket(domain, tpe, protocol)
  )

  def fcntl(fd: Int, cmd: Int, arg: Int): Int = ret(pfcntl.fcntl(fd, cmd, arg))

  def close(fd: Int): Int = ret(nativesocket.close(fd))

  def bind(fd: Int, addr: Array[Byte], addrLen: Int): Int =
    ret(nativesocket.bind(fd, ptr(addr, 0), addrLen.toUInt))

  def listen(fd: Int, backlog: Int): Int = ret(nativesocket.listen(fd, backlog))

  def connect(fd: Int, addr: Array[Byte], addrLen: Int): Int =
    ret(nativesocket.connect(fd, ptr(addr, 0), addrLen.toUInt))

  def accept(fd: Int, flags: Int): Int =
    if (LinktimeInfo.isLinux) ret(nativesocket.accept4(fd, null, null, flags))
    else ret(nativesocket.accept(fd, null, null))

  def getsockname(fd: Int, addr: Array[Byte], addrLen: Array[Int]): Int =
    ret(nativesocket.getsockname(fd, ptr(addr, 0), lenPtr(addrLen)))

  def getpeername(fd: Int, addr: Array[Byte], addrLen: Array[Int]): Int =
    ret(nativesocket.getpeername(fd, ptr(addr, 0), lenPtr(addrLen)))

  def getsockopt(fd: Int, level: Int, name: Int, value: Array[Byte], valueLen: Array[Int]): Int =
    ret(nativesocket.getsockopt(fd, level, name, ptr(value, 0), lenPtr(valueLen)))

  def setsockopt(fd: Int, level: Int, name: Int, value: Array[Byte], valueLen: Int): Int =
    ret(nativesocket.setsockopt(fd, level, name, ptr(value, 0), valueLen.toUInt))

  def read(fd: Int, buf: Array[Byte], offset: Int, length: Int): Int =
    retS(nativesocket.read(fd, ptr(buf, offset), length.toUSize))

  def write(fd: Int, buf: Array[Byte], offset: Int, length: Int): Int =
    if (LinktimeInfo.isLinux)
      retS(nativesocket.send(fd, ptr(buf, offset), length.toUSize, constants.MSG_NOSIGNAL))
    else retS(nativesocket.write(fd, ptr(buf, offset), length.toUSize))

  def recvfrom(
      fd: Int,
      buf: Array[Byte],
      offset: Int,
      length: Int,
      flags: Int,
      addr: Array[Byte],
      addrLen: Array[Int]
  ): Int =
    retS(
      nativesocket.recvfrom(
        fd,
        ptr(buf, offset),
        length.toUSize,
        flags,
        ptr(addr, 0),
        if (addrLen eq null) null else lenPtr(addrLen)
      )
    )

  def sendto(
      fd: Int,
      buf: Array[Byte],
      offset: Int,
      length: Int,
      flags: Int,
      addr: Array[Byte],
      addrLen: Int
  ): Int =
    retS(
      nativesocket.sendto(fd, ptr(buf, offset), length.toUSize, flags, ptr(addr, 0), addrLen.toUInt)
    )

  def shutdown(fd: Int, how: Int): Int = ret(nativesocket.shutdown(fd, how))

  def strerror(errno: Int): String = fromCString(cstrerror(errno))
}

@nowarn212("cat=unused")
@extern
private[internal] object nativesocket {
  def socket(domain: CInt, tpe: CInt, protocol: CInt): CInt = extern
  // May block: `close` with a positive SO_LINGER, and `bind`/`connect` on a Unix socket's file.
  // `@blocking` lets the GC run meanwhile.
  @blocking def close(fd: CInt): CInt = extern
  @blocking def bind(fd: CInt, addr: Ptr[Byte], addrLen: CUnsignedInt): CInt = extern
  def listen(fd: CInt, backlog: CInt): CInt = extern
  @blocking def connect(fd: CInt, addr: Ptr[Byte], addrLen: CUnsignedInt): CInt = extern
  def accept(fd: CInt, addr: Ptr[Byte], addrLen: Ptr[CUnsignedInt]): CInt = extern
  // Linux and FreeBSD only; only referenced behind LinktimeInfo.isLinux
  def accept4(fd: CInt, addr: Ptr[Byte], addrLen: Ptr[CUnsignedInt], flags: CInt): CInt = extern
  def getsockname(fd: CInt, addr: Ptr[Byte], addrLen: Ptr[CUnsignedInt]): CInt = extern
  def getpeername(fd: CInt, addr: Ptr[Byte], addrLen: Ptr[CUnsignedInt]): CInt = extern
  def getsockopt(
      fd: CInt,
      level: CInt,
      name: CInt,
      value: Ptr[Byte],
      valueLen: Ptr[CUnsignedInt]
  ): CInt = extern
  def setsockopt(
      fd: CInt,
      level: CInt,
      name: CInt,
      value: Ptr[Byte],
      valueLen: CUnsignedInt
  ): CInt =
    extern
  def read(fd: CInt, buf: Ptr[Byte], count: CSize): CSSize = extern
  def write(fd: CInt, buf: Ptr[Byte], count: CSize): CSSize = extern
  def send(fd: CInt, buf: Ptr[Byte], count: CSize, flags: CInt): CSSize = extern
  def recvfrom(
      fd: CInt,
      buf: Ptr[Byte],
      count: CSize,
      flags: CInt,
      addr: Ptr[Byte],
      addrLen: Ptr[CUnsignedInt]
  ): CSSize = extern
  def sendto(
      fd: CInt,
      buf: Ptr[Byte],
      count: CSize,
      flags: CInt,
      addr: Ptr[Byte],
      addrLen: CUnsignedInt
  ): CSSize = extern
  def shutdown(fd: CInt, how: CInt): CInt = extern
}
