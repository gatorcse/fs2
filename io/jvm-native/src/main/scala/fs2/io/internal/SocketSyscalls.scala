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

/** The raw socket system calls that fs2's poller-based sockets need, so that the socket logic in
  * `FdPollingSocket` and friends can be shared between Scala Native (`@extern`) and the JVM (the
  * Foreign Function & Memory API, in `fs2-io-ffm`).
  *
  * The interface deals only in ints and heap arrays, so that every implementation is a thin,
  * allocation-free binding:
  *
  *   - Every call returns what the C function returns on success, or `-errno` on failure.
  *     Callers interpret `errno` with [[SocketConstants]] (see [[FdSockets]]).
  *   - Socket addresses, option values and other structs are passed as byte arrays laid out as
  *     the C structs for this OS, encoded and decoded by [[FdSockets]]. `len` arguments that C
  *     takes by pointer (`socklen_t *`) are a one-element `Array[Int]`.
  *   - Buffers are heap arrays with an offset and a length. Implementations must not retain
  *     them past the call. The JVM implementation passes them to the kernel without a copy, using
  *     critical (pinned) heap access; on Scala Native the GC doesn't move objects.
  *
  * Every call must be non-blocking except `close`: callers only ever invoke them on non-blocking
  * sockets.
  *
  * This is an internal SPI, not a public API. The JVM finds an implementation with
  * `java.util.ServiceLoader`.
  */
private[fs2] trait SocketSyscalls {

  /** The constants (address families, option names, errno values) for this OS. */
  def constants: SocketConstants

  def socket(domain: Int, tpe: Int, protocol: Int): Int

  /** `fcntl(fd, cmd, arg)` with an `int` argument (e.g. `F_GETFL`, `F_SETFL`, `F_SETFD`). */
  def fcntl(fd: Int, cmd: Int, arg: Int): Int

  def close(fd: Int): Int

  def bind(fd: Int, addr: Array[Byte], addrLen: Int): Int

  def listen(fd: Int, backlog: Int): Int

  def connect(fd: Int, addr: Array[Byte], addrLen: Int): Int

  /** `accept4(fd, NULL, NULL, flags)` where the OS has it, else `accept(fd, NULL, NULL)` and
    * `flags` must be zero.
    */
  def accept(fd: Int, flags: Int): Int

  def getsockname(fd: Int, addr: Array[Byte], addrLen: Array[Int]): Int

  def getpeername(fd: Int, addr: Array[Byte], addrLen: Array[Int]): Int

  def getsockopt(fd: Int, level: Int, name: Int, value: Array[Byte], valueLen: Array[Int]): Int

  def setsockopt(fd: Int, level: Int, name: Int, value: Array[Byte], valueLen: Int): Int

  /** `read(2)` into `buf[offset, offset + length)`. */
  def read(fd: Int, buf: Array[Byte], offset: Int, length: Int): Int

  /** `send(2)` from `buf[offset, offset + length)`, never raising `SIGPIPE`: `MSG_NOSIGNAL` on
    * Linux, plain `write(2)` where the socket has `SO_NOSIGPIPE` set instead (macOS).
    */
  def write(fd: Int, buf: Array[Byte], offset: Int, length: Int): Int

  /** `recvfrom(2)`. `addr` must hold a `sockaddr_storage`; `addrLen(0)` is its size on entry and
    * the address length on return.
    */
  def recvfrom(
      fd: Int,
      buf: Array[Byte],
      offset: Int,
      length: Int,
      flags: Int,
      addr: Array[Byte],
      addrLen: Array[Int]
  ): Int

  /** `sendto(2)`; a `null` `addr` sends to the connected peer. */
  def sendto(
      fd: Int,
      buf: Array[Byte],
      offset: Int,
      length: Int,
      flags: Int,
      addr: Array[Byte],
      addrLen: Int
  ): Int

  def shutdown(fd: Int, how: Int): Int

  /** The OS's message for `errno`, as `strerror(3)`. */
  def strerror(errno: Int): String
}
