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

import cats.effect.FileDescriptorPollHandle
import cats.effect.IO
import cats.effect.LiftIO
import cats.effect.kernel.Async
import cats.effect.kernel.Resource
import cats.syntax.all._
import com.comcast.ip4s._
import fs2.io.internal.FdSockets

import java.util.concurrent.atomic.AtomicInteger

import FdPollingIpSocketsProvider.{SoMaxConn, accepted}

private final class FdPollingIpSocketsProvider[F[_]: Dns: LiftIO](fds: FdSockets)(implicit
    F: Async[F]
) extends IpSocketsProvider[F] {

  private[this] val c = fds.c

  override def connectIp(
      to: SocketAddress[Host],
      options: List[SocketOption]
  ): Resource[F, Socket[F]] = for {
    poller <- Resource.eval(fileDescriptorPoller[F])
    address <- Resource.eval(to.resolve)
    domain = fds.domainOf(address)
    fd <- fds.openNonBlocking(domain, c.SOCK_STREAM)
    _ <- Resource.eval(fds.setOptionsF(fd, domain, options))
    handle <- poller.registerFileDescriptor(fd, true, true).mapK(LiftIO.liftK)
    _ <- Resource.eval {
      handle
        .pollWriteRec(false) { connecting =>
          IO {
            if (connecting) {
              // Writability doesn't prove the connection completed: the poller may wake us
              // early, e.g. for an event from before connect was called.
              if (fds.finishConnect(fd)) Either.unit else Left(true)
            } else if (fds.connect(fd, address)) Either.unit
            else Left(true) // we will be connected when we unblock
          }
        }
        .to
    }
    addresses <- Resource.eval(F.delay((fds.localAddress(fd), fds.peerAddress(fd))))
    socket <- FdPollingSocket[F](fds, fd, domain, handle, addresses._1, addresses._2)
  } yield socket

  override def bindIp(
      address: SocketAddress[Host],
      options: List[SocketOption]
  ): Resource[F, ServerSocket[F]] = for {
    poller <- Resource.eval(fileDescriptorPoller[F])
    (filteredOptions, backlog) = SocketOption.extractBacklog(options)
    resolvedHost <- Resource.eval(address.host.resolve)
    opened <- fds.openForBind(resolvedHost, c.SOCK_STREAM)
    (fd, domain) = opened
    handle <- poller.registerFileDescriptor(fd, true, false).mapK(LiftIO.liftK)
    serverGate <- FdGate.resource[F]

    _ <- Resource.eval {
      F.delay(fds.setInt(fd, c.SOL_SOCKET, c.SO_REUSEADDR, 1)) *>
        F.delay(fds.bind(fd, SocketAddress(resolvedHost, address.port), domain)) *>
        F.delay(fds.listen(fd, backlog.getOrElse(SoMaxConn)))
    }
    localAddress <- Resource.eval(F.delay(fds.localAddress(fd)))

    sockets = Stream
      .resource {
        // errors (e.g. out of descriptors, or an unsupported option) end the stream, like NIO's;
        // only a client that has already gone is dropped
        accepted[F](fds, fd, handle, serverGate)
          .flatMap { clientFd =>
            val connected = F.delay(Option(fds.peerAddressIfConnected(clientFd))).flatMap {
              case None       => F.pure(Option.empty[GenSocketAddress])
              case Some(peer) =>
                fds
                  .setAcceptedOptionsF(clientFd, domain, filteredOptions)
                  .map(ok => if (ok) Some(peer) else None)
            }
            Resource.eval(connected).flatMap {
              case None       => Resource.pure[F, Option[Socket[F]]](None)
              case Some(peer) =>
                for {
                  local <- Resource.eval(F.delay(fds.localAddress(clientFd)))
                  handle <- poller
                    .registerFileDescriptor(clientFd, true, true)
                    .mapK(LiftIO.liftK)
                  socket <- FdPollingSocket[F](fds, clientFd, domain, handle, local, peer)
                } yield Some(socket)
            }
          }
      }
      .repeat
      .unNone

    info = new SocketInfo[F] {
      def getOption[A](key: SocketOption.Key[A]) =
        F.delay(serverGate(fds.getOption(fd, domain, key)))
      def setOption[A](key: SocketOption.Key[A], value: A) =
        F.delay(serverGate(fds.setOption(fd, domain, key, value)))
      def supportedOptions = F.pure(fds.supportedOptions(datagram = false, domain))
      val address = localAddress
    }
  } yield ServerSocket(info, sockets)

}

private object FdPollingIpSocketsProvider {

  /** `SOMAXCONN`; the kernel clamps larger values to its own limit. */
  private[net] final val SoMaxConn = 4096

  /** The next connection accepted on the listening socket `fd`, closed when released. */
  private[net] def accepted[F[_]: LiftIO](
      fds: FdSockets,
      fd: Int,
      handle: FileDescriptorPollHandle,
      serverGate: FdGate
  )(implicit F: Async[F]): Resource[F, Int] =
    Resource.makeFull[F, Int] { poll =>
      F.delay(new AtomicInteger(-1)).flatMap { taken =>
        val accept = poll {
          handle
            .pollReadRec(()) { _ =>
              IO {
                val clientFd = serverGate(fds.accept(fd))
                if (clientFd >= 0) {
                  taken.set(clientFd)
                  Right(clientFd)
                } else Left(())
              }
            }
            .to
        }
        // canceled after accept(2) but before the resource took ownership of the descriptor
        F.onCancel(
          accept,
          F.delay {
            val clientFd = taken.getAndSet(-1)
            if (clientFd >= 0) fds.close(clientFd)
          }
        )
      }
    }(clientFd => F.delay(fds.close(clientFd)))
}
