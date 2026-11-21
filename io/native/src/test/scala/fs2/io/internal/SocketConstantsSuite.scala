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

package fs2
package io
package internal

import scala.scalanative.meta.LinktimeInfo
import scala.scalanative.posix.{errno => e}
import scala.scalanative.posix.fcntl
import scala.scalanative.posix.netinet.{in => netin}
import scala.scalanative.posix.netinet.tcp
import scala.scalanative.posix.sys.socket

/** Checks the shared [[SocketConstants]] tables, which the JVM (FFM) bindings use too, against
  * the C headers as Scala Native sees them on this OS.
  */
class SocketConstantsSuite extends Fs2Suite {
  private val c = NativeSocketSyscalls.constants

  private def check(name: String, ours: Int, theirs: Int)(implicit loc: munit.Location): Unit =
    assertEquals(ours, theirs, name)

  test("the table for this OS") {
    assertEquals(c.isLinux, LinktimeInfo.isLinux)
    assertEquals(c.isMac, LinktimeInfo.isMac)
  }

  test("address families, socket types and levels") {
    check("AF_UNSPEC", c.AF_UNSPEC, socket.AF_UNSPEC)
    check("AF_UNIX", c.AF_UNIX, socket.AF_UNIX)
    check("AF_INET", c.AF_INET, socket.AF_INET)
    check("AF_INET6", c.AF_INET6, socket.AF_INET6)
    check("SOCK_STREAM", c.SOCK_STREAM, socket.SOCK_STREAM)
    check("SOCK_DGRAM", c.SOCK_DGRAM, socket.SOCK_DGRAM)
    check("SOL_SOCKET", c.SOL_SOCKET, socket.SOL_SOCKET)
    check("IPPROTO_IP", c.IPPROTO_IP, netin.IPPROTO_IP)
    check("IPPROTO_TCP", c.IPPROTO_TCP, netin.IPPROTO_TCP)
    check("IPPROTO_IPV6", c.IPPROTO_IPV6, netin.IPPROTO_IPV6)
    check("SHUT_RD", c.SHUT_RD, socket.SHUT_RD)
    check("SHUT_WR", c.SHUT_WR, socket.SHUT_WR)
    if (LinktimeInfo.isLinux) check("MSG_NOSIGNAL", c.MSG_NOSIGNAL, socket.MSG_NOSIGNAL)
  }

  test("socket options") {
    check("SO_REUSEADDR", c.SO_REUSEADDR, socket.SO_REUSEADDR)
    check("SO_REUSEPORT", c.SO_REUSEPORT, socket.SO_REUSEPORT)
    check("SO_KEEPALIVE", c.SO_KEEPALIVE, socket.SO_KEEPALIVE)
    check("SO_BROADCAST", c.SO_BROADCAST, socket.SO_BROADCAST)
    // macOS uses SO_LINGER_SEC (seconds), which Scala Native doesn't define
    if (LinktimeInfo.isLinux) check("SO_LINGER", c.SO_LINGER, socket.SO_LINGER)
    check("SO_SNDBUF", c.SO_SNDBUF, socket.SO_SNDBUF)
    check("SO_RCVBUF", c.SO_RCVBUF, socket.SO_RCVBUF)
    check("SO_ERROR", c.SO_ERROR, socket.SO_ERROR)
    check("TCP_NODELAY", c.TCP_NODELAY, tcp.TCP_NODELAY)
    check("IP_MULTICAST_IF", c.IP_MULTICAST_IF, netin.IP_MULTICAST_IF)
    check("IP_MULTICAST_LOOP", c.IP_MULTICAST_LOOP, netin.IP_MULTICAST_LOOP)
    check("IPV6_MULTICAST_IF", c.IPV6_MULTICAST_IF, netin.IPV6_MULTICAST_IF)
    check("IPV6_MULTICAST_HOPS", c.IPV6_MULTICAST_HOPS, netin.IPV6_MULTICAST_HOPS)
    check("IPV6_MULTICAST_LOOP", c.IPV6_MULTICAST_LOOP, netin.IPV6_MULTICAST_LOOP)
  }

  test("fcntl") {
    check("F_SETFD", c.F_SETFD, fcntl.F_SETFD)
    check("F_GETFL", c.F_GETFL, fcntl.F_GETFL)
    check("F_SETFL", c.F_SETFL, fcntl.F_SETFL)
    check("FD_CLOEXEC", c.FD_CLOEXEC, fcntl.FD_CLOEXEC)
    check("O_NONBLOCK", c.O_NONBLOCK, fcntl.O_NONBLOCK)
  }

  test("errno") {
    check("EINTR", c.EINTR, e.EINTR)
    check("EAGAIN", c.EAGAIN, e.EAGAIN)
    check("EWOULDBLOCK", c.EAGAIN, e.EWOULDBLOCK)
    check("EINPROGRESS", c.EINPROGRESS, e.EINPROGRESS)
    check("EALREADY", c.EALREADY, e.EALREADY)
    check("EISCONN", c.EISCONN, e.EISCONN)
    check("ENOTCONN", c.ENOTCONN, e.ENOTCONN)
    check("ENOPROTOOPT", c.ENOPROTOOPT, e.ENOPROTOOPT)
    check("EAFNOSUPPORT", c.EAFNOSUPPORT, e.EAFNOSUPPORT)
    check("EADDRINUSE", c.EADDRINUSE, e.EADDRINUSE)
    check("EADDRNOTAVAIL", c.EADDRNOTAVAIL, e.EADDRNOTAVAIL)
    check("ECONNREFUSED", c.ECONNREFUSED, e.ECONNREFUSED)
    check("ECONNRESET", c.ECONNRESET, e.ECONNRESET)
    check("EPIPE", c.EPIPE, e.EPIPE)
    check("ETIMEDOUT", c.ETIMEDOUT, e.ETIMEDOUT)
    check("ENETUNREACH", c.ENETUNREACH, e.ENETUNREACH)
    check("EHOSTUNREACH", c.EHOSTUNREACH, e.EHOSTUNREACH)
  }

  test("sockaddr round trip through getsockname") {
    val fds = NativeSocketSyscalls.sockets
    val fd = fds.guard(NativeSocketSyscalls.socket(c.AF_INET, c.SOCK_DGRAM, 0))
    try {
      val loopback = com.comcast.ip4s.SocketAddress(
        com.comcast.ip4s.Ipv4Address.fromBytes(127, 0, 0, 1),
        com.comcast.ip4s.Port.Wildcard
      )
      fds.bind(fd, loopback)
      val bound = fds.localAddress(fd).asIpUnsafe
      assertEquals(bound.host, loopback.host)
      assert(bound.port.value > 0)
    } finally fds.close(fd)
  }

  // Scala Native's own struct definitions aren't the C layouts (its sockaddr_storage is 256
  // bytes), so the layouts are checked by round trips through the kernel instead
  test("unix sockaddr round trip through getsockname") {
    val fds = NativeSocketSyscalls.sockets
    val path = s"fs2-socket-constants-${System.nanoTime()}.sock"
    val fd = fds.guard(NativeSocketSyscalls.socket(c.AF_UNIX, c.SOCK_DGRAM, 0))
    try {
      fds.bind(fd, com.comcast.ip4s.UnixSocketAddress(path))
      assertEquals(fds.localAddress(fd), com.comcast.ip4s.UnixSocketAddress(path))
    } finally {
      fds.close(fd)
      java.nio.file.Files.deleteIfExists(java.nio.file.Paths.get(path)): Unit
    }
  }
}
