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

import cats.effect.kernel.{Resource, Sync}
import cats.syntax.all._
import com.comcast.ip4s.{
  GenSocketAddress,
  IpAddress,
  Ipv4Address,
  Ipv6Address,
  Port,
  SocketAddress,
  UnixSocketAddress
}

import java.io.IOException
import java.net.{
  BindException,
  ConnectException,
  NoRouteToHostException,
  PortUnreachableException,
  SocketOption,
  StandardSocketOptions,
  NetworkInterface => JNetworkInterface
}
import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets

/** The socket logic shared by Scala Native and the JVM on top of [[SocketSyscalls]]: errno
  * handling, `sockaddr` encoding, socket options and multicast membership.
  *
  * Methods without an effect type are unsafe: callers run them inside `F.delay` or `IO`.
  */
private[fs2] final class FdSockets(val sys: SocketSyscalls) {
  import FdSockets._

  val c: SocketConstants = sys.constants

  // errno

  def errnoToThrowable(errno: Int): Throwable = {
    val msg = sys.strerror(errno)
    if (errno == c.EADDRINUSE || errno == c.EADDRNOTAVAIL) new BindException(msg)
    else if (errno == c.ECONNREFUSED || errno == c.ETIMEDOUT) new ConnectException(msg)
    else if (errno == c.EHOSTUNREACH || errno == c.ENETUNREACH) new NoRouteToHostException(msg)
    else new IOException(s"errno($errno) $msg")
  }

  /** Runs a system call, retrying on `EINTR`. Returns its result, or -1 if it would block; throws
    * on any other error.
    */
  @inline def guard(call: => Int): Int = {
    var r = call
    while (r == -c.EINTR) r = call
    if (r >= 0) r
    else if (c.wouldBlock(-r)) -1
    else throw errnoToThrowable(-r)
  }

  @inline def guard_(call: => Int): Unit = {
    guard(call)
    ()
  }

  /** Like [[guard]], but returns `-errno` instead of throwing when `mask(errno)`. */
  @inline def guardMask(call: => Int)(mask: Int => Boolean): Int = {
    var r = call
    while (r == -c.EINTR) r = call
    if (r >= 0 || mask(-r)) r
    else throw errnoToThrowable(-r)
  }

  // descriptors

  /** Opens a non-blocking, close-on-exec socket (that never raises `SIGPIPE` on macOS), closed
    * when the resource is released.
    */
  def openNonBlocking[F[_]](domain: Int, tpe: Int)(implicit F: Sync[F]): Resource[F, Int] =
    Resource.make(F.delay(openRaw(domain, tpe)))(fd => F.delay(close(fd)))

  /** Opens a socket to bind to `host`. The IPv4 wildcard opens a dual-stack IPv6 socket, like
    * the JDK, so that a server on the default address also accepts IPv6 clients; it falls back to
    * IPv4 where IPv6 is unavailable. Returns the descriptor and its address family.
    */
  def openForBind[F[_]](host: IpAddress, tpe: Int)(implicit F: Sync[F]): Resource[F, (Int, Int)] =
    if (host == Ipv4Address.Wildcard)
      Resource.make {
        F.delay {
          var fd = sys.socket(c.AF_INET6, tpe | c.SOCK_NONBLOCK_CLOEXEC, 0)
          if (fd == -c.EAFNOSUPPORT) (openRaw(c.AF_INET, tpe), c.AF_INET)
          else {
            fd = guard(fd)
            try {
              configure(fd)
              setInt(fd, c.IPPROTO_IPV6, c.IPV6_V6ONLY, 0)
            } catch {
              case t: Throwable =>
                sys.close(fd)
                throw t
            }
            (fd, c.AF_INET6)
          }
        }
      }(fd => F.delay(close(fd._1)))
    else {
      val domain = if (host.isInstanceOf[Ipv4Address]) c.AF_INET else c.AF_INET6
      openNonBlocking(domain, tpe).map((_, domain))
    }

  private[this] def openRaw(domain: Int, tpe: Int): Int = {
    val fd = guard(sys.socket(domain, tpe | c.SOCK_NONBLOCK_CLOEXEC, 0))
    try configure(fd)
    catch {
      case t: Throwable =>
        sys.close(fd)
        throw t
    }
    fd
  }

  /** Closes `fd`. `EINTR` is not retried: the descriptor is released either way on Linux and
    * macOS, and it may already be reused.
    */
  def close(fd: Int): Unit = {
    val r = sys.close(fd)
    if (r < 0 && r != -c.EINTR) throw errnoToThrowable(-r)
  }

  /** Accepts a pending connection as a non-blocking, close-on-exec socket, or returns -1 if none
    * is pending.
    */
  def accept(fd: Int): Int = {
    // ECONNABORTED: a pending connection was reset before we took it; take the next one
    def accept0() =
      guardMask(sys.accept(fd, c.SOCK_NONBLOCK_CLOEXEC))(e => e == c.ECONNABORTED || c.wouldBlock(e))
    var client = accept0()
    while (client == -c.ECONNABORTED) client = accept0()
    if (client < 0) client = -1 // would block
    if (client >= 0)
      try configure(client)
      catch {
        case t: Throwable =>
          sys.close(client)
          throw t
      }
    client
  }

  /** What `SOCK_NONBLOCK | SOCK_CLOEXEC` don't do on this OS. */
  private[this] def configure(fd: Int): Unit =
    if (c.SOCK_NONBLOCK_CLOEXEC == 0) {
      val flags = guard(sys.fcntl(fd, c.F_GETFL, 0))
      guard_(sys.fcntl(fd, c.F_SETFL, flags | c.O_NONBLOCK))
      guard_(sys.fcntl(fd, c.F_SETFD, c.FD_CLOEXEC))
      if (c.SO_NOSIGPIPE >= 0) setInt(fd, c.SOL_SOCKET, c.SO_NOSIGPIPE, 1)
    }

  def setNonBlocking(fd: Int): Unit = {
    val flags = guard(sys.fcntl(fd, c.F_GETFL, 0))
    guard_(sys.fcntl(fd, c.F_SETFL, flags | c.O_NONBLOCK))
  }

  def bind(fd: Int, address: GenSocketAddress): Unit = bind(fd, address, familyOfAddress(address))

  /** Binds `fd`, a socket of address family `domain`. */
  def bind(fd: Int, address: GenSocketAddress, domain: Int): Unit = {
    val addr = encode(address, domain)
    guard_(sys.bind(fd, addr, addr.length))
  }

  def listen(fd: Int, backlog: Int): Unit = guard_(sys.listen(fd, backlog))

  /** Starts connecting. Returns `true` if connected, `false` if the connection is in progress
    * (wait for writability, then call [[finishConnect]]).
    */
  def connect(fd: Int, address: GenSocketAddress): Boolean =
    connect(fd, address, familyOfAddress(address))

  /** [[connect]] on a socket of address family `domain`. Only `EINPROGRESS` means the connection
    * is under way: `EAGAIN` (a full Unix listen backlog, or no free local port) is an error.
    */
  def connect(fd: Int, address: GenSocketAddress, domain: Int): Boolean = {
    val addr = encode(address, domain)
    guardMask(sys.connect(fd, addr, addr.length))(_ == c.EINPROGRESS) >= 0
  }

  /** After a non-blocking connect signalled writability: throws the connect error if there was
    * one, and returns whether the connection is now established. Writability alone doesn't
    * prove it: the poller may wake us for an event from before connect was called.
    */
  def finishConnect(fd: Int): Boolean = {
    checkSocketError(fd)
    isConnected(fd)
  }

  /** Throws the pending error on `fd` (`SO_ERROR`), if any. */
  def checkSocketError(fd: Int): Unit = {
    val err = getInt(fd, c.SOL_SOCKET, c.SO_ERROR)
    if (err != 0) throw errnoToThrowable(err)
  }

  /** Whether a non-blocking `connect` has completed. `getpeername` fails with `ENOTCONN` while
    * the connection is still being established.
    */
  def isConnected(fd: Int): Boolean = {
    val buf = new Array[Byte](c.SockaddrStorageSize)
    val len = Array(buf.length)
    guardMask(sys.getpeername(fd, buf, len))(_ == c.ENOTCONN) >= 0
  }

  def shutdown(fd: Int, how: Int): Unit = {
    guardMask(sys.shutdown(fd, how))(_ == c.ENOTCONN)
    ()
  }

  def localAddress(fd: Int): GenSocketAddress = {
    val buf = new Array[Byte](c.SockaddrStorageSize)
    val len = Array(buf.length)
    guard_(sys.getsockname(fd, buf, len))
    decode(buf, len(0))
  }

  def peerAddress(fd: Int): GenSocketAddress = {
    val buf = new Array[Byte](c.SockaddrStorageSize)
    val len = Array(buf.length)
    guard_(sys.getpeername(fd, buf, len))
    decode(buf, len(0))
  }

  def domainOf(address: SocketAddress[IpAddress]): Int =
    if (address.host.isInstanceOf[Ipv4Address]) c.AF_INET else c.AF_INET6

  private[this] def familyOfAddress(address: GenSocketAddress): Int = address match {
    case ip: SocketAddress[?] =>
      if (ip.host.isInstanceOf[Ipv4Address]) c.AF_INET else c.AF_INET6
    case _ => c.AF_UNIX
  }

  // sockaddr

  def encode(address: GenSocketAddress): Array[Byte] = encode(address, familyOfAddress(address))

  /** Encodes `address` for a socket of address family `domain`: an IPv4 address for an IPv6
    * (dual-stack) socket becomes IPv4-mapped, and the IPv4 wildcard becomes `::`.
    */
  def encode(address: GenSocketAddress, domain: Int): Array[Byte] = address match {
    case ip: SocketAddress[?] =>
      ip.host match {
        case v4: Ipv4Address if domain == c.AF_INET6 =>
          encodeIn6(
            if (v4 == Ipv4Address.Wildcard) Ipv6Address.fromBytes(new Array[Byte](16)).get
            else v4.toMappedV6,
            ip.port
          )
        case v4: Ipv4Address => encodeIn(v4, ip.port)
        case v6: Ipv6Address => encodeIn6(v6, ip.port)
        case other           =>
          throw new IllegalArgumentException(s"Unresolved address: $other")
      }
    case unix: UnixSocketAddress => encodeUn(unix.path)
    case other => throw new IllegalArgumentException(s"Unsupported address: $other")
  }

  private[this] def putFamily(buf: Array[Byte], family: Int, len: Int): Unit =
    if (c.sockaddrHasLen) {
      buf(0) = len.toByte
      buf(1) = family.toByte
    } else {
      // sa_family_t is a native-endian u16
      if (NativeLittleEndian) {
        buf(0) = family.toByte
        buf(1) = (family >>> 8).toByte
      } else {
        buf(0) = (family >>> 8).toByte
        buf(1) = family.toByte
      }
    }

  def familyOf(buf: Array[Byte]): Int =
    if (c.sockaddrHasLen) buf(1) & 0xff
    else if (NativeLittleEndian) (buf(0) & 0xff) | ((buf(1) & 0xff) << 8)
    else ((buf(0) & 0xff) << 8) | (buf(1) & 0xff)

  private[this] def encodeIn(host: Ipv4Address, port: Port): Array[Byte] = {
    val buf = new Array[Byte](c.SockaddrInSize)
    encodeIn(buf, 0, host, port)
    buf
  }

  private[this] def encodeIn(buf: Array[Byte], at: Int, host: Ipv4Address, port: Port): Unit = {
    putFamily0(buf, at, c.AF_INET, c.SockaddrInSize)
    buf(at + 2) = (port.value >>> 8).toByte
    buf(at + 3) = port.value.toByte
    System.arraycopy(host.toBytes, 0, buf, at + 4, 4)
  }

  private[this] def encodeIn6(host: Ipv6Address, port: Port): Array[Byte] = {
    val buf = new Array[Byte](c.SockaddrIn6Size)
    encodeIn6(buf, 0, host, port)
    buf
  }

  private[this] def encodeIn6(buf: Array[Byte], at: Int, host: Ipv6Address, port: Port): Unit = {
    putFamily0(buf, at, c.AF_INET6, c.SockaddrIn6Size)
    buf(at + 2) = (port.value >>> 8).toByte
    buf(at + 3) = port.value.toByte
    // sin6_flowinfo (4 bytes) stays 0
    System.arraycopy(host.toBytes, 0, buf, at + 8, 16)
    // sin6_scope_id (4 bytes) stays 0
  }

  private[this] def putFamily0(buf: Array[Byte], at: Int, family: Int, len: Int): Unit =
    if (at == 0) putFamily(buf, family, len)
    else {
      val tmp = new Array[Byte](2)
      putFamily(tmp, family, len)
      buf(at) = tmp(0)
      buf(at + 1) = tmp(1)
    }

  private[this] def encodeUn(path: String): Array[Byte] = {
    val pathBytes = path.getBytes(StandardCharsets.UTF_8)
    val abstractNamespace = pathBytes.nonEmpty && pathBytes(0) == 0
    // a pathname needs room for its terminating NUL; an abstract name (Linux) has none
    if (pathBytes.length > c.sunPathSize - (if (abstractNamespace) 0 else 1))
      throw new IllegalArgumentException(s"Path too long: $path")
    val len = if (abstractNamespace) 2 + pathBytes.length else c.SockaddrUnSize
    val buf = new Array[Byte](len)
    putFamily(buf, c.AF_UNIX, len)
    System.arraycopy(pathBytes, 0, buf, 2, pathBytes.length)
    buf
  }

  def decode(buf: Array[Byte], len: Int): GenSocketAddress = {
    val family = familyOf(buf)
    if (family == c.AF_INET) {
      val port = ((buf(2) & 0xff) << 8) | (buf(3) & 0xff)
      val host = Ipv4Address.fromBytes(buf(4) & 0xff, buf(5) & 0xff, buf(6) & 0xff, buf(7) & 0xff)
      SocketAddress(host, Port.fromInt(port).get)
    } else if (family == c.AF_INET6) {
      val port = ((buf(2) & 0xff) << 8) | (buf(3) & 0xff)
      val host = Ipv6Address.fromBytes(java.util.Arrays.copyOfRange(buf, 8, 24)).get
      // like the JDK, a dual-stack socket's IPv4 peers are IPv4 addresses
      SocketAddress(host.collapseMappedV4, Port.fromInt(port).get)
    } else if (family == c.AF_UNIX) {
      if (len > 2 && buf(2) == 0)
        // an abstract name (Linux): every byte up to `len` is part of it
        UnixSocketAddress(new String(buf, 2, len - 2, StandardCharsets.UTF_8))
      else {
        // unnamed sockets have no path at all
        var end = 2
        while (end < len && buf(end) != 0) end += 1
        UnixSocketAddress(new String(buf, 2, end - 2, StandardCharsets.UTF_8))
      }
    } else throw new UnsupportedOperationException(s"Unsupported address family $family")
  }

  // socket options

  def getInt(fd: Int, level: Int, name: Int): Int = {
    val buf = new Array[Byte](4)
    val len = Array(4)
    guard_(sys.getsockopt(fd, level, name, buf, len))
    decodeOptionInt(buf, len(0))
  }

  /** `None` if the option isn't available on this socket (`ENOPROTOOPT`, or `EOPNOTSUPP` for an
    * option of another protocol).
    */
  def getIntOption(fd: Int, level: Int, name: Int): Option[Int] = {
    val buf = new Array[Byte](4)
    val len = Array(4)
    if (guardMask(sys.getsockopt(fd, level, name, buf, len))(unsupportedOption) < 0) None
    else Some(decodeOptionInt(buf, len(0)))
  }

  private[this] def unsupportedOption(errno: Int): Boolean =
    errno == c.ENOPROTOOPT || errno == c.EOPNOTSUPP || errno == c.ENOTSUP

  def setInt(fd: Int, level: Int, name: Int, value: Int): Unit = {
    val buf = intBytes(value)
    guard_(sys.setsockopt(fd, level, name, buf, 4))
  }

  /** Some options (IPv4 multicast TTL and loop) are a `u_char` on BSDs; Linux accepts both. */
  private[this] def setByte(fd: Int, level: Int, name: Int, value: Int): Unit =
    guard_(sys.setsockopt(fd, level, name, Array(value.toByte), 1))

  private[this] def decodeOptionInt(buf: Array[Byte], len: Int): Int =
    if (len == 1) buf(0) & 0xff
    else ByteBuffer.wrap(buf).order(ByteOrder.nativeOrder()).getInt(0)

  /** The options a socket of this kind supports. */
  def supportedOptions(datagram: Boolean, domain: Int): Set[SocketOption[?]] =
    if (datagram) DatagramOptions
    else if (domain == c.AF_UNIX) UnixStreamOptions
    else if (c.isLinux) StreamOptions + fs2.io.net.SocketOption.OriginalDestination
    else StreamOptions

  /** The value of `key` on `fd`, a socket of address family `domain`. `None` if the option
    * isn't supported.
    */
  def getOption[A](fd: Int, domain: Int, key: SocketOption[A]): Option[A] = {
    val v6 = domain == c.AF_INET6
    def bool(level: Int, name: Int): Option[Any] =
      getIntOption(fd, level, name).map(i => java.lang.Boolean.valueOf(i != 0))
    def int(level: Int, name: Int): Option[Any] =
      getIntOption(fd, level, name).map(i => java.lang.Integer.valueOf(i))

    val result: Option[Any] = key match {
      case StandardSocketOptions.SO_SNDBUF    => int(c.SOL_SOCKET, c.SO_SNDBUF)
      case StandardSocketOptions.SO_RCVBUF    => int(c.SOL_SOCKET, c.SO_RCVBUF)
      case StandardSocketOptions.SO_REUSEADDR => bool(c.SOL_SOCKET, c.SO_REUSEADDR)
      case StandardSocketOptions.SO_REUSEPORT => bool(c.SOL_SOCKET, c.SO_REUSEPORT)
      case StandardSocketOptions.SO_KEEPALIVE => bool(c.SOL_SOCKET, c.SO_KEEPALIVE)
      case StandardSocketOptions.SO_BROADCAST => bool(c.SOL_SOCKET, c.SO_BROADCAST)
      case StandardSocketOptions.TCP_NODELAY  => bool(c.IPPROTO_TCP, c.TCP_NODELAY)
      case StandardSocketOptions.SO_LINGER    =>
        val buf = new Array[Byte](8)
        val len = Array(8)
        guard_(sys.getsockopt(fd, c.SOL_SOCKET, c.SO_LINGER, buf, len))
        val bb = ByteBuffer.wrap(buf).order(ByteOrder.nativeOrder())
        // like the JDK: -1 when disabled
        Some(java.lang.Integer.valueOf(if (bb.getInt(0) == 0) -1 else bb.getInt(4)))
      case StandardSocketOptions.IP_TOS =>
        if (v6) int(c.IPPROTO_IPV6, c.IPV6_TCLASS) else int(c.IPPROTO_IP, c.IP_TOS)
      case StandardSocketOptions.IP_MULTICAST_TTL =>
        if (v6) int(c.IPPROTO_IPV6, c.IPV6_MULTICAST_HOPS)
        else int(c.IPPROTO_IP, c.IP_MULTICAST_TTL)
      case StandardSocketOptions.IP_MULTICAST_LOOP =>
        if (v6) bool(c.IPPROTO_IPV6, c.IPV6_MULTICAST_LOOP)
        else bool(c.IPPROTO_IP, c.IP_MULTICAST_LOOP)
      case StandardSocketOptions.IP_MULTICAST_IF =>
        if (v6)
          getIntOption(fd, c.IPPROTO_IPV6, c.IPV6_MULTICAST_IF)
            .filter(_ != 0)
            .flatMap(i => Option(JNetworkInterface.getByIndex(i)))
        else {
          val buf = new Array[Byte](4)
          val len = Array(4)
          guard_(sys.getsockopt(fd, c.IPPROTO_IP, c.IP_MULTICAST_IF, buf, len))
          if (buf.forall(_ == 0)) None
          else Option(JNetworkInterface.getByInetAddress(java.net.InetAddress.getByAddress(buf)))
        }
      case fs2.io.net.SocketOption.OriginalDestination =>
        if (c.SO_ORIGINAL_DST < 0) None
        else {
          val buf = new Array[Byte](c.SockaddrStorageSize)
          val len = Array(buf.length)
          // ENOENT: the connection isn't redirected
          val r = guardMask(sys.getsockopt(fd, c.IPPROTO_IP, c.SO_ORIGINAL_DST, buf, len)) { e =>
            e == c.ENOPROTOOPT || e == 2 /* ENOENT, on every supported OS */
          }
          if (r < 0) None else Some(decode(buf, len(0)))
        }
      case _ => None
    }
    result.asInstanceOf[Option[A]]
  }

  /** Sets `key` on `fd`, a socket of address family `domain`. */
  def setOption[A](fd: Int, domain: Int, key: SocketOption[A], value: A): Unit = {
    val v6 = domain == c.AF_INET6
    def bool = if (value.asInstanceOf[java.lang.Boolean].booleanValue) 1 else 0
    def int = value.asInstanceOf[java.lang.Integer].intValue
    key match {
      case StandardSocketOptions.SO_SNDBUF    => setInt(fd, c.SOL_SOCKET, c.SO_SNDBUF, int)
      case StandardSocketOptions.SO_RCVBUF    => setInt(fd, c.SOL_SOCKET, c.SO_RCVBUF, int)
      case StandardSocketOptions.SO_REUSEADDR =>
        setInt(fd, c.SOL_SOCKET, c.SO_REUSEADDR, bool)
        // Like the JDK: on BSDs, SO_REUSEADDR alone doesn't let datagram sockets share a port
        if (c.isMac && getInt(fd, c.SOL_SOCKET, c.SO_TYPE) == c.SOCK_DGRAM)
          setInt(fd, c.SOL_SOCKET, c.SO_REUSEPORT, bool)
      case StandardSocketOptions.SO_REUSEPORT => setInt(fd, c.SOL_SOCKET, c.SO_REUSEPORT, bool)
      case StandardSocketOptions.SO_KEEPALIVE => setInt(fd, c.SOL_SOCKET, c.SO_KEEPALIVE, bool)
      case StandardSocketOptions.SO_BROADCAST => setInt(fd, c.SOL_SOCKET, c.SO_BROADCAST, bool)
      case StandardSocketOptions.TCP_NODELAY  => setInt(fd, c.IPPROTO_TCP, c.TCP_NODELAY, bool)
      case StandardSocketOptions.SO_LINGER    =>
        val bb = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder())
        if (int >= 0) bb.putInt(0, 1).putInt(4, int) else bb.putInt(0, 0).putInt(4, 0)
        guard_(sys.setsockopt(fd, c.SOL_SOCKET, c.SO_LINGER, bb.array, 8))
      // Linux sends IPv4 traffic from a dual-stack socket with its IPv4 options, so, like the
      // JDK, an IPv6 socket there also gets the IPv4 option (best effort)
      case StandardSocketOptions.IP_TOS =>
        if (v6) {
          setInt(fd, c.IPPROTO_IPV6, c.IPV6_TCLASS, int)
          if (c.isLinux) bestEffort(fd, c.IPPROTO_IP, c.IP_TOS, intBytes(int))
        } else setInt(fd, c.IPPROTO_IP, c.IP_TOS, int)
      case StandardSocketOptions.IP_MULTICAST_TTL =>
        if (v6) {
          setInt(fd, c.IPPROTO_IPV6, c.IPV6_MULTICAST_HOPS, int)
          if (c.isLinux) bestEffort(fd, c.IPPROTO_IP, c.IP_MULTICAST_TTL, intBytes(int))
        } else setByte(fd, c.IPPROTO_IP, c.IP_MULTICAST_TTL, int)
      case StandardSocketOptions.IP_MULTICAST_LOOP =>
        if (v6) {
          setInt(fd, c.IPPROTO_IPV6, c.IPV6_MULTICAST_LOOP, bool)
          if (c.isLinux) bestEffort(fd, c.IPPROTO_IP, c.IP_MULTICAST_LOOP, intBytes(bool))
        } else setByte(fd, c.IPPROTO_IP, c.IP_MULTICAST_LOOP, bool)
      case StandardSocketOptions.IP_MULTICAST_IF =>
        val iface = value.asInstanceOf[JNetworkInterface]
        if (v6) {
          setInt(fd, c.IPPROTO_IPV6, c.IPV6_MULTICAST_IF, iface.getIndex)
          if (c.isLinux) ipv4AddressOf(iface).foreach { addr =>
            bestEffort(fd, c.IPPROTO_IP, c.IP_MULTICAST_IF, addr.toBytes)
          }
        } else {
          val addr = ipv4AddressOf(iface).getOrElse(
            throw new IOException(s"Interface ${iface.getName} has no IPv4 address")
          )
          guard_(sys.setsockopt(fd, c.IPPROTO_IP, c.IP_MULTICAST_IF, addr.toBytes, 4))
        }
      case _ =>
        throw new IllegalArgumentException(s"Unsupported socket option: ${key.name}")
    }
  }

  /** Sets an option, ignoring failure. */
  private[this] def bestEffort(fd: Int, level: Int, name: Int, value: Array[Byte]): Unit = {
    sys.setsockopt(fd, level, name, value, value.length)
    ()
  }

  private[this] def ipv4AddressOf(iface: JNetworkInterface): Option[Ipv4Address] = {
    val it = iface.getInetAddresses
    var found: Option[Ipv4Address] = None
    while (found.isEmpty && it.hasMoreElements)
      it.nextElement() match {
        case a: java.net.Inet4Address => found = Ipv4Address.fromBytes(a.getAddress)
        case _                        => ()
      }
    found
  }

  // multicast

  /** One of the `MCAST_*` operations, with `group_req` (no `source`) or `group_source_req`. */
  def multicast(
      fd: Int,
      op: Int,
      interfaceIndex: Int,
      group: IpAddress,
      source: Option[IpAddress]
  ): Unit = {
    val level = if (group.isInstanceOf[Ipv4Address]) c.IPPROTO_IP else c.IPPROTO_IPV6
    val size = if (source.isDefined) c.GroupSourceReqSize else c.GroupReqSize
    val buf = new Array[Byte](size)
    val bb = ByteBuffer.wrap(buf).order(ByteOrder.nativeOrder())
    bb.putInt(0, interfaceIndex)
    putIp(buf, c.groupReqAddrOffset, group)
    source.foreach(putIp(buf, c.groupReqAddrOffset + c.SockaddrStorageSize, _))
    guard_(sys.setsockopt(fd, level, op, buf, size))
  }

  private[this] def putIp(buf: Array[Byte], at: Int, ip: IpAddress): Unit =
    ip.fold(encodeIn(buf, at, _, Port.Wildcard), encodeIn6(buf, at, _, Port.Wildcard))

  // datagrams

  /** `recvfrom` into `buf`. Returns the sender, or `null` if no datagram is pending (a
    * zero-length datagram still has a sender). The length is in `lengthOut(0)`.
    */
  def recvFrom(
      fd: Int,
      buf: Array[Byte],
      offset: Int,
      length: Int,
      lengthOut: Array[Int]
  ): GenSocketAddress = {
    val addr = new Array[Byte](c.SockaddrStorageSize)
    val addrLen = Array(addr.length)
    val n = guardMask(sys.recvfrom(fd, buf, offset, length, 0, addr, addrLen)) { e =>
      c.wouldBlock(e) || e == c.ECONNREFUSED
    }
    if (n >= 0) {
      lengthOut(0) = n
      decode(addr, addrLen(0))
    } else if (n == -c.ECONNREFUSED)
      // a connected datagram socket's peer has no listener (ICMP port unreachable)
      throw new PortUnreachableException(sys.strerror(c.ECONNREFUSED))
    else null
  }

  /** `sendto` (or `send` to the connected peer when `address` is `null`). Returns the bytes sent,
    * or -1 if it would block.
    */
  def sendTo(
      fd: Int,
      domain: Int,
      buf: Array[Byte],
      offset: Int,
      length: Int,
      address: GenSocketAddress
  ): Int = {
    val mask = (e: Int) => e == c.ECONNREFUSED || c.wouldBlock(e)
    val r =
      if (address eq null)
        guardMask(sys.sendto(fd, buf, offset, length, c.MSG_NOSIGNAL, null, 0))(mask)
      else {
        val addr = encode(address, domain)
        guardMask(sys.sendto(fd, buf, offset, length, c.MSG_NOSIGNAL, addr, addr.length))(mask)
      }
    if (r == -c.ECONNREFUSED)
      // a connected datagram socket's peer has no listener (ICMP port unreachable)
      throw new PortUnreachableException(sys.strerror(c.ECONNREFUSED))
    else if (r < 0) -1 // would block
    else r
  }

  /** Dissolves a datagram socket's association (`connect` to `AF_UNSPEC`). */
  def disconnect(fd: Int): Unit = {
    val addr = new Array[Byte](c.SockaddrIn6Size)
    putFamily(addr, c.AF_UNSPEC, addr.length)
    // BSDs report EAFNOSUPPORT, but have dissolved the association
    guardMask(sys.connect(fd, addr, addr.length))(_ == c.EAFNOSUPPORT)
    ()
  }

  // effectful wrappers

  def getOptionF[F[_], A](fd: Int, domain: Int, key: SocketOption[A])(implicit
      F: Sync[F]
  ): F[Option[A]] = F.delay(getOption(fd, domain, key))

  def setOptionF[F[_], A](fd: Int, domain: Int, key: SocketOption[A], value: A)(implicit
      F: Sync[F]
  ): F[Unit] = F.delay(setOption(fd, domain, key, value))

  def setOptionsF[F[_]](fd: Int, domain: Int, options: List[fs2.io.net.SocketOption])(implicit
      F: Sync[F]
  ): F[Unit] = options.traverse_(so => setOptionF(fd, domain, so.key, so.value))
}

private[fs2] object FdSockets {
  private val NativeLittleEndian = ByteOrder.nativeOrder() == ByteOrder.LITTLE_ENDIAN

  private def intBytes(value: Int): Array[Byte] =
    ByteBuffer.allocate(4).order(ByteOrder.nativeOrder()).putInt(0, value).array

  private val StreamOptions: Set[SocketOption[?]] = Set(
    StandardSocketOptions.SO_SNDBUF,
    StandardSocketOptions.SO_RCVBUF,
    StandardSocketOptions.SO_REUSEADDR,
    StandardSocketOptions.SO_REUSEPORT,
    StandardSocketOptions.SO_KEEPALIVE,
    StandardSocketOptions.SO_LINGER,
    StandardSocketOptions.TCP_NODELAY,
    StandardSocketOptions.IP_TOS
  )

  private val UnixStreamOptions: Set[SocketOption[?]] = Set(
    StandardSocketOptions.SO_SNDBUF,
    StandardSocketOptions.SO_RCVBUF,
    StandardSocketOptions.SO_LINGER
  )

  private val DatagramOptions: Set[SocketOption[?]] = Set(
    StandardSocketOptions.SO_SNDBUF,
    StandardSocketOptions.SO_RCVBUF,
    StandardSocketOptions.SO_REUSEADDR,
    StandardSocketOptions.SO_REUSEPORT,
    StandardSocketOptions.SO_BROADCAST,
    StandardSocketOptions.IP_TOS,
    StandardSocketOptions.IP_MULTICAST_IF,
    StandardSocketOptions.IP_MULTICAST_TTL,
    StandardSocketOptions.IP_MULTICAST_LOOP
  )
}
