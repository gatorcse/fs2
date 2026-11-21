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

/** The C constants and struct layouts that fs2's poller-based sockets need, for one OS.
  *
  * They are a property of the OS (and its ABI), not of how it is called, so Scala Native and the
  * JVM share these tables. Only Linux (x86_64 and aarch64, the generic ABI) and macOS are covered,
  * matching the platforms that have a `FileDescriptorPoller`.
  */
private[fs2] final class SocketConstants private (
    val isLinux: Boolean,
    val isMac: Boolean,
    // address families and socket types
    val AF_UNSPEC: Int,
    val AF_UNIX: Int,
    val AF_INET: Int,
    val AF_INET6: Int,
    val SOCK_STREAM: Int,
    val SOCK_DGRAM: Int,
    // `SOCK_NONBLOCK | SOCK_CLOEXEC` for `socket` and `accept4`, or 0 where the OS lacks them.
    val SOCK_NONBLOCK_CLOEXEC: Int,
    // fcntl
    val F_SETFD: Int,
    val F_GETFL: Int,
    val F_SETFL: Int,
    val FD_CLOEXEC: Int,
    val O_NONBLOCK: Int,
    // send flags and shutdown
    val MSG_NOSIGNAL: Int,
    val SHUT_RD: Int,
    val SHUT_WR: Int,
    // levels
    val SOL_SOCKET: Int,
    val IPPROTO_IP: Int,
    val IPPROTO_TCP: Int,
    val IPPROTO_IPV6: Int,
    // SOL_SOCKET options
    val SO_REUSEADDR: Int,
    val SO_REUSEPORT: Int,
    val SO_KEEPALIVE: Int,
    val SO_BROADCAST: Int,
    // `SO_LINGER` in seconds (`SO_LINGER_SEC` on macOS, whose `SO_LINGER` counts ticks).
    val SO_LINGER: Int,
    val SO_SNDBUF: Int,
    val SO_RCVBUF: Int,
    val SO_ERROR: Int,
    val SO_TYPE: Int,
    // macOS only; -1 elsewhere.
    val SO_NOSIGPIPE: Int,
    // IPPROTO_TCP
    val TCP_NODELAY: Int,
    // keep-alive tuning (`jdk.net.ExtendedSocketOptions`); `TCP_KEEPALIVE` is macOS's idle time
    val TCP_KEEPIDLE: Int,
    val TCP_KEEPINTVL: Int,
    val TCP_KEEPCNT: Int,
    // Linux only; -1 elsewhere.
    val TCP_QUICKACK: Int,
    // IPPROTO_IP
    val IP_TOS: Int,
    val IP_MULTICAST_IF: Int,
    val IP_MULTICAST_TTL: Int,
    val IP_MULTICAST_LOOP: Int,
    // Linux netfilter only; -1 elsewhere.
    val SO_ORIGINAL_DST: Int,
    // IPPROTO_IPV6
    val IPV6_TCLASS: Int,
    val IPV6_MULTICAST_IF: Int,
    val IPV6_MULTICAST_HOPS: Int,
    val IPV6_MULTICAST_LOOP: Int,
    val IPV6_V6ONLY: Int,
    // `ipv6_mreq` joins, for IPv4 groups on a dual-stack socket on macOS
    val IPV6_JOIN_GROUP: Int,
    val IPV6_LEAVE_GROUP: Int,
    // protocol-independent multicast (RFC 3678), at IPPROTO_IP or IPPROTO_IPV6
    val MCAST_JOIN_GROUP: Int,
    val MCAST_LEAVE_GROUP: Int,
    val MCAST_JOIN_SOURCE_GROUP: Int,
    val MCAST_LEAVE_SOURCE_GROUP: Int,
    val MCAST_BLOCK_SOURCE: Int,
    val MCAST_UNBLOCK_SOURCE: Int,
    // errno
    val EINTR: Int,
    val EAGAIN: Int,
    val EINPROGRESS: Int,
    val EALREADY: Int,
    val EISCONN: Int,
    val ENOTCONN: Int,
    val ENOPROTOOPT: Int,
    val EAFNOSUPPORT: Int,
    val EOPNOTSUPP: Int,
    val ENOTSUP: Int,
    val ECONNABORTED: Int,
    val EADDRINUSE: Int,
    val EADDRNOTAVAIL: Int,
    val ECONNREFUSED: Int,
    val ECONNRESET: Int,
    val EPIPE: Int,
    val ETIMEDOUT: Int,
    val ENETUNREACH: Int,
    val EHOSTUNREACH: Int,
    // struct layouts
    // BSD `sockaddr`s start with a one-byte length, and the family is one byte.
    val sockaddrHasLen: Boolean,
    // `sizeof(((struct sockaddr_un *)0)->sun_path)`.
    val sunPathSize: Int,
    // Offset of `gr_group` in `struct group_req` (and `gsr_group` in `group_source_req`).
    val groupReqAddrOffset: Int
) {
  val SockaddrStorageSize = 128
  val SockaddrInSize = 16
  val SockaddrIn6Size = 28
  val SockaddrUnSize = 2 + sunPathSize
  val GroupReqSize = groupReqAddrOffset + SockaddrStorageSize
  val GroupSourceReqSize = groupReqAddrOffset + 2 * SockaddrStorageSize

  // `EWOULDBLOCK` is `EAGAIN` on both Linux and macOS.
  def wouldBlock(errno: Int): Boolean = errno == EAGAIN
}

private[fs2] object SocketConstants {

  // Linux, generic ABI (x86_64, aarch64, riscv64; not MIPS, SPARC, PowerPC or Alpha).
  val Linux: SocketConstants = new SocketConstants(
    isLinux = true,
    isMac = false,
    AF_UNSPEC = 0,
    AF_UNIX = 1,
    AF_INET = 2,
    AF_INET6 = 10,
    SOCK_STREAM = 1,
    SOCK_DGRAM = 2,
    SOCK_NONBLOCK_CLOEXEC = 0x800 | 0x80000,
    F_SETFD = 2,
    F_GETFL = 3,
    F_SETFL = 4,
    FD_CLOEXEC = 1,
    O_NONBLOCK = 0x800,
    MSG_NOSIGNAL = 0x4000,
    SHUT_RD = 0,
    SHUT_WR = 1,
    SOL_SOCKET = 1,
    IPPROTO_IP = 0,
    IPPROTO_TCP = 6,
    IPPROTO_IPV6 = 41,
    SO_REUSEADDR = 2,
    SO_REUSEPORT = 15,
    SO_KEEPALIVE = 9,
    SO_BROADCAST = 6,
    SO_LINGER = 13,
    SO_SNDBUF = 7,
    SO_RCVBUF = 8,
    SO_ERROR = 4,
    SO_TYPE = 3,
    SO_NOSIGPIPE = -1,
    TCP_NODELAY = 1,
    TCP_KEEPIDLE = 4,
    TCP_KEEPINTVL = 5,
    TCP_KEEPCNT = 6,
    TCP_QUICKACK = 12,
    IP_TOS = 1,
    IP_MULTICAST_IF = 32,
    IP_MULTICAST_TTL = 33,
    IP_MULTICAST_LOOP = 34,
    SO_ORIGINAL_DST = 80,
    IPV6_TCLASS = 67,
    IPV6_MULTICAST_IF = 17,
    IPV6_MULTICAST_HOPS = 18,
    IPV6_MULTICAST_LOOP = 19,
    IPV6_V6ONLY = 26,
    IPV6_JOIN_GROUP = 20,
    IPV6_LEAVE_GROUP = 21,
    MCAST_JOIN_GROUP = 42,
    MCAST_LEAVE_GROUP = 45,
    MCAST_JOIN_SOURCE_GROUP = 46,
    MCAST_LEAVE_SOURCE_GROUP = 47,
    MCAST_BLOCK_SOURCE = 43,
    MCAST_UNBLOCK_SOURCE = 44,
    EINTR = 4,
    EAGAIN = 11,
    EINPROGRESS = 115,
    EALREADY = 114,
    EISCONN = 106,
    ENOTCONN = 107,
    ENOPROTOOPT = 92,
    EAFNOSUPPORT = 97,
    EOPNOTSUPP = 95,
    ENOTSUP = 95,
    ECONNABORTED = 103,
    EADDRINUSE = 98,
    EADDRNOTAVAIL = 99,
    ECONNREFUSED = 111,
    ECONNRESET = 104,
    EPIPE = 32,
    ETIMEDOUT = 110,
    ENETUNREACH = 101,
    EHOSTUNREACH = 113,
    sockaddrHasLen = false,
    sunPathSize = 108,
    // struct group_req { uint32_t gr_interface; struct sockaddr_storage gr_group; },
    // sockaddr_storage is 8-byte aligned
    groupReqAddrOffset = 8
  )

  // macOS (Darwin), arm64 and x86_64.
  val Darwin: SocketConstants = new SocketConstants(
    isLinux = false,
    isMac = true,
    AF_UNSPEC = 0,
    AF_UNIX = 1,
    AF_INET = 2,
    AF_INET6 = 30,
    SOCK_STREAM = 1,
    SOCK_DGRAM = 2,
    SOCK_NONBLOCK_CLOEXEC = 0,
    F_SETFD = 2,
    F_GETFL = 3,
    F_SETFL = 4,
    FD_CLOEXEC = 1,
    O_NONBLOCK = 0x4,
    MSG_NOSIGNAL = 0,
    SHUT_RD = 0,
    SHUT_WR = 1,
    SOL_SOCKET = 0xffff,
    IPPROTO_IP = 0,
    IPPROTO_TCP = 6,
    IPPROTO_IPV6 = 41,
    SO_REUSEADDR = 0x4,
    SO_REUSEPORT = 0x200,
    SO_KEEPALIVE = 0x8,
    SO_BROADCAST = 0x20,
    SO_LINGER = 0x1080, // SO_LINGER_SEC
    SO_SNDBUF = 0x1001,
    SO_RCVBUF = 0x1002,
    SO_ERROR = 0x1007,
    SO_TYPE = 0x1008,
    SO_NOSIGPIPE = 0x1022,
    TCP_NODELAY = 1,
    TCP_KEEPIDLE = 0x10,
    TCP_KEEPINTVL = 0x101,
    TCP_KEEPCNT = 0x102,
    TCP_QUICKACK = -1,
    IP_TOS = 3,
    IP_MULTICAST_IF = 9,
    IP_MULTICAST_TTL = 10,
    IP_MULTICAST_LOOP = 11,
    SO_ORIGINAL_DST = -1,
    IPV6_TCLASS = 36,
    IPV6_MULTICAST_IF = 9,
    IPV6_MULTICAST_HOPS = 10,
    IPV6_MULTICAST_LOOP = 11,
    IPV6_V6ONLY = 27,
    IPV6_JOIN_GROUP = 12,
    IPV6_LEAVE_GROUP = 13,
    MCAST_JOIN_GROUP = 80,
    MCAST_LEAVE_GROUP = 81,
    MCAST_JOIN_SOURCE_GROUP = 82,
    MCAST_LEAVE_SOURCE_GROUP = 83,
    MCAST_BLOCK_SOURCE = 84,
    MCAST_UNBLOCK_SOURCE = 85,
    EINTR = 4,
    EAGAIN = 35,
    EINPROGRESS = 36,
    EALREADY = 37,
    EISCONN = 56,
    ENOTCONN = 57,
    ENOPROTOOPT = 42,
    EAFNOSUPPORT = 47,
    EOPNOTSUPP = 102,
    ENOTSUP = 45,
    ECONNABORTED = 53,
    EADDRINUSE = 48,
    EADDRNOTAVAIL = 49,
    ECONNREFUSED = 61,
    ECONNRESET = 54,
    EPIPE = 32,
    ETIMEDOUT = 60,
    ENETUNREACH = 51,
    EHOSTUNREACH = 65,
    sockaddrHasLen = true,
    sunPathSize = 104,
    // <netinet/in.h> declares group_req under #pragma pack(4)
    groupReqAddrOffset = 4
  )
}
