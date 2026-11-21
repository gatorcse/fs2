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
package net

import cats.effect.{Async, FileDescriptorPollHandle, IO, LiftIO, Resource}
import cats.effect.std.Mutex
import cats.syntax.all._
import com.comcast.ip4s.{
  Dns,
  GenSocketAddress,
  IpAddress,
  MulticastJoin,
  NetworkInterface,
  SocketAddress
}
import fs2.io.internal.FdSockets

import java.io.IOException
import java.net.{NetworkInterface => JNetworkInterface}

import FdPollingDatagramSocket.MaxDatagramSize

/** A datagram socket on a `FileDescriptorPoller`, shared by Scala Native and the JVM (with
  * `fs2-io-ffm`).
  */
private final class FdPollingDatagramSocket[F[_]: LiftIO] private (
    fds: FdSockets,
    fd: Int,
    domain: Int,
    handle: FileDescriptorPollHandle,
    gate: FdGate,
    readMutex: Mutex[F],
    writeMutex: Mutex[F],
    override val address: SocketAddress[IpAddress]
)(implicit F: Async[F])
    extends DatagramSocket[F] {

  private[this] val c = fds.c

  /** The peer, while connected. A connected socket sends to it without an address: macOS refuses
    * (`EISCONN`) a `sendto` with an address on a connected socket, even the peer's.
    */
  @volatile private[this] var peer: GenSocketAddress = null

  def localAddress: F[SocketAddress[IpAddress]] = F.pure(address)

  def read: F[Datagram] = readMutex.lock.surround {
    handle
      .pollReadRec(()) { _ =>
        IO {
          // the largest possible datagram; copied out before this fiber can suspend
          val buf = FdPollingSocket.scratchBuffer(MaxDatagramSize)
          val length = new Array[Int](1)
          val from =
            gate(fds.recvFrom(fd, buf, 0, math.min(buf.length, MaxDatagramSize), length))
          // a zero-length datagram still has a sender
          if (from eq null) Left(())
          else
            Right(
              Datagram(from.asIpUnsafe, Chunk.array(java.util.Arrays.copyOf(buf, length(0))))
            )
        }
      }
      .to
  }

  def readGen: F[GenDatagram] = read.map(_.toGenDatagram)

  def reads: Stream[F, Datagram] = Stream.repeatEval(read)

  private[this] def write0(bytes: Chunk[Byte], to: GenSocketAddress): F[Unit] =
    writeMutex.lock.surround {
      val Chunk.ArraySlice(buf, offset, length) = bytes.toArraySlice
      handle
        .pollWriteRec(()) { _ =>
          IO {
            // a datagram is sent whole or not at all
            val sent = gate {
              val connectedTo = peer
              // like NIO; the OSes differ here (Linux sends anyway, macOS fails with EISCONN)
              val dest =
                if (connectedTo eq null) {
                  if (to eq null) throw DatagramConnectionErrors.notYetConnected()
                  to
                } else if ((to eq null) || connectedTo == to) null
                else throw DatagramConnectionErrors.alreadyConnected()
              fds.sendTo(fd, domain, buf, offset, length, dest)
            }
            if (sent >= 0) Either.unit else Left(())
          }
        }
        .to
    }

  def write(bytes: Chunk[Byte], address: GenSocketAddress): F[Unit] = write0(bytes, address)

  def write(bytes: Chunk[Byte]): F[Unit] = write0(bytes, null)

  def writes: Pipe[F, Datagram, Nothing] = _.evalMap(write).drain

  def connect(address: GenSocketAddress): F[Unit] =
    writeMutex.lock.surround {
      F.delay {
        gate {
          // connecting a datagram socket only sets its peer, so it never blocks
          if (!fds.connect(fd, address, domain))
            throw new IOException(s"Datagram connect to $address did not complete")
          peer = address
        }
      }
    }

  def disconnect: F[Unit] = writeMutex.lock.surround {
    F.delay {
      gate {
        fds.disconnect(fd)
        peer = null
        // Dissolving the association can unbind the socket: Linux drops a port it picked itself,
        // and macOS a specific local address. Like the JDK, bind it again where it was.
        if (fds.localAddress(fd) != address) fds.bind(fd, address, domain)
      }
    }
  }

  def getOption[A](key: SocketOption.Key[A]): F[Option[A]] =
    F.delay(gate(fds.getOption(fd, domain, key)))

  def setOption[A](key: SocketOption.Key[A], value: A): F[Unit] =
    F.delay(gate(fds.setOption(fd, domain, key, value)))

  def supportedOptions: F[Set[SocketOption.Key[?]]] =
    F.pure(fds.supportedOptions(datagram = true, domain))

  override def join(
      join: MulticastJoin[IpAddress],
      interface: NetworkInterface
  ): F[GroupMembership] =
    F.delay(Option(JNetworkInterface.getByName(interface.name))).flatMap {
      case Some(jinterface) => this.join(join, jinterface)
      case None => F.raiseError(new IOException(s"No such network interface: ${interface.name}"))
    }

  override def join(
      join: MulticastJoin[IpAddress],
      interface: JNetworkInterface
  ): F[GroupMembership] =
    F.delay {
      val index = interface.getIndex
      val (group, source) = join.fold(
        j => (j.group.address: IpAddress, None),
        j => (j.group.address: IpAddress, Some(j.source: IpAddress))
      )
      gate {
        fds.multicast(
          fd,
          domain,
          if (source.isEmpty) c.MCAST_JOIN_GROUP else c.MCAST_JOIN_SOURCE_GROUP,
          index,
          group,
          source
        )
      }
      new GroupMembership {
        def drop = F.delay {
          gate.apply(
            fds.multicast(
              fd,
              domain,
              if (source.isEmpty) c.MCAST_LEAVE_GROUP else c.MCAST_LEAVE_SOURCE_GROUP,
              index,
              group,
              source
            )
          )
        }
        def block(blocked: IpAddress) = F.delay {
          if (source.isDefined)
            throw new IllegalStateException("Cannot block sources of a source-specific join")
          gate(fds.multicast(fd, domain, c.MCAST_BLOCK_SOURCE, index, group, Some(blocked)))
        }
        def unblock(blocked: IpAddress) = F.delay {
          if (source.isDefined)
            throw new IllegalStateException("Cannot unblock sources of a source-specific join")
          gate(fds.multicast(fd, domain, c.MCAST_UNBLOCK_SOURCE, index, group, Some(blocked)))
        }
        override def toString = "GroupMembership"
      }
    }
}

private object FdPollingDatagramSocket {

  /** The largest UDP payload over IPv4 or IPv6 (without jumbograms). */
  private final val MaxDatagramSize = 65535

  def apply[F[_]: LiftIO](
      fds: FdSockets,
      fd: Int,
      domain: Int,
      handle: FileDescriptorPollHandle,
      address: SocketAddress[IpAddress]
  )(implicit F: Async[F]): Resource[F, DatagramSocket[F]] = for {
    mutexes <- Resource.eval((Mutex[F], Mutex[F]).tupled)
    gate <- FdGate.resource[F]
  } yield new FdPollingDatagramSocket(
    fds,
    fd,
    domain,
    handle,
    gate,
    mutexes._1,
    mutexes._2,
    address
  )
}

private final class FdPollingIpDatagramSocketsProvider[F[_]: Dns: LiftIO](fds: FdSockets)(implicit
    F: Async[F]
) extends IpDatagramSocketsProvider[F] {

  override def bindDatagramSocket(
      address: SocketAddress[com.comcast.ip4s.Host],
      options: List[SocketOption]
  ): Resource[F, DatagramSocket[F]] = for {
    poller <- Resource.eval(fileDescriptorPoller[F])
    host <- Resource.eval(address.host.resolve[F])
    opened <- fds.openForBind(host, fds.c.SOCK_DGRAM)
    (fd, domain) = opened
    _ <- Resource.eval(fds.setOptionsF(fd, domain, options))
    _ <- Resource.eval(F.delay(fds.bind(fd, SocketAddress(host, address.port), domain)))
    handle <- poller.registerFileDescriptor(fd, true, true).mapK(LiftIO.liftK)
    localAddress <- Resource.eval(F.delay(fds.localAddress(fd).asIpUnsafe))
    socket <- FdPollingDatagramSocket[F](fds, fd, domain, handle, localAddress)
  } yield socket
}
