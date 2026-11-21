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

package fs2.io.ffm

import fs2.io.internal.{SocketConstants, SocketSyscalls}
import fs2.io.ffm.internal.SocketCalls

/** [[fs2.io.internal.SocketSyscalls]] for the JVM, through the Foreign Function & Memory API
  * (JDK 25+). Found by `fs2-io` through `java.util.ServiceLoader`, so having this module on the
  * classpath is enough: `Network[IO]` then uses poller-based sockets whenever the runtime's
  * polling system is a `FileDescriptorPoller`, such as `cats.effect.ffm.FfmPollingSystem()`.
  *
  * The JVM needs native access for it, e.g. `--enable-native-access=ALL-UNNAMED`.
  *
  * Supports Linux (x86_64, aarch64) and macOS; constructing it fails elsewhere, and `fs2-io`
  * then keeps using NIO.
  */
final class FfmSocketSyscalls extends SocketSyscalls {

  val constants: SocketConstants =
    if (SocketCalls.IS_LINUX) SocketConstants.Linux
    else if (SocketCalls.IS_MAC) SocketConstants.Darwin
    else throw new UnsupportedOperationException(s"fs2-io-ffm doesn't support ${SocketCalls.OS}")

  private[this] val msgNoSignal = constants.MSG_NOSIGNAL

  def socket(domain: Int, tpe: Int, protocol: Int): Int = SocketCalls.socket(domain, tpe, protocol)

  def fcntl(fd: Int, cmd: Int, arg: Int): Int = SocketCalls.fcntl(fd, cmd, arg)

  def close(fd: Int): Int = SocketCalls.close(fd)

  def bind(fd: Int, addr: Array[Byte], addrLen: Int): Int = SocketCalls.bind(fd, addr, addrLen)

  def listen(fd: Int, backlog: Int): Int = SocketCalls.listen(fd, backlog)

  def connect(fd: Int, addr: Array[Byte], addrLen: Int): Int =
    SocketCalls.connect(fd, addr, addrLen)

  def accept(fd: Int, flags: Int): Int = SocketCalls.accept(fd, flags)

  def getsockname(fd: Int, addr: Array[Byte], addrLen: Array[Int]): Int =
    SocketCalls.getsockname(fd, addr, addrLen)

  def getpeername(fd: Int, addr: Array[Byte], addrLen: Array[Int]): Int =
    SocketCalls.getpeername(fd, addr, addrLen)

  def getsockopt(fd: Int, level: Int, name: Int, value: Array[Byte], valueLen: Array[Int]): Int =
    SocketCalls.getsockopt(fd, level, name, value, valueLen)

  def setsockopt(fd: Int, level: Int, name: Int, value: Array[Byte], valueLen: Int): Int =
    SocketCalls.setsockopt(fd, level, name, value, valueLen)

  def read(fd: Int, buf: Array[Byte], offset: Int, length: Int): Int =
    SocketCalls.read(fd, buf, offset, length)

  def write(fd: Int, buf: Array[Byte], offset: Int, length: Int): Int =
    SocketCalls.write(fd, buf, offset, length, msgNoSignal)

  def recvfrom(
      fd: Int,
      buf: Array[Byte],
      offset: Int,
      length: Int,
      flags: Int,
      addr: Array[Byte],
      addrLen: Array[Int]
  ): Int = SocketCalls.recvfrom(fd, buf, offset, length, flags, addr, addrLen)

  def sendto(
      fd: Int,
      buf: Array[Byte],
      offset: Int,
      length: Int,
      flags: Int,
      addr: Array[Byte],
      addrLen: Int
  ): Int = SocketCalls.sendto(fd, buf, offset, length, flags, addr, addrLen)

  def shutdown(fd: Int, how: Int): Int = SocketCalls.shutdown(fd, how)

  def strerror(errno: Int): String = SocketCalls.strerror(errno)
}
