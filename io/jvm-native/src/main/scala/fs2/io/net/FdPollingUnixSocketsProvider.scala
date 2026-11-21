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

import cats.effect.{Async, IO, LiftIO, Resource}
import cats.syntax.all._

import com.comcast.ip4s.UnixSocketAddress

import fs2.io.file.Files
import fs2.io.internal.FdSockets

import FdPollingIpSocketsProvider.{SoMaxConn, accepted}

private final class FdPollingUnixSocketsProvider[F[_]: Files: LiftIO](fds: FdSockets)(implicit
    F: Async[F]
) extends UnixSocketsProvider[F] {

  private[this] val c = fds.c

  override def connectUnix(
      address: UnixSocketAddress,
      options: List[SocketOption]
  ): Resource[F, Socket[F]] =
    for {
      poller <- Resource.eval(fileDescriptorPoller[F])
      fd <- fds.openNonBlocking(c.AF_UNIX, c.SOCK_STREAM)
      _ <- Resource.eval(fds.setOptionsF(fd, c.AF_UNIX, options))
      handle <- poller.registerFileDescriptor(fd, true, true).mapK(LiftIO.liftK)
      _ <- Resource.eval {
        handle
          .pollWriteRec(false) { connecting =>
            IO {
              if (connecting) {
                // writability alone doesn't prove the connection completed
                if (fds.finishConnect(fd)) Either.unit else Left(true)
              } else if (fds.connect(fd, address)) Either.unit
              else Left(true) // we will be connected when unblocked
            }
          }
          .to
      }
      socket <- FdPollingSocket[F](
        fds,
        fd,
        c.AF_UNIX,
        handle,
        UnixSocketAddress(""),
        address
      )
    } yield socket

  override def bindUnix(
      address: UnixSocketAddress,
      options: List[SocketOption]
  ): Resource[F, ServerSocket[F]] = {

    val (optionsWithoutDeletes, deleteIfExists, deleteOnClose) =
      SocketOption.extractUnixSocketDeletes(options, address)
    val (filteredOptions, backlog) = SocketOption.extractBacklog(optionsWithoutDeletes)

    (for {
      poller <- Resource.eval(fileDescriptorPoller[F])

      _ <- Resource.eval(deleteIfExists)

      fd <- fds.openNonBlocking(c.AF_UNIX, c.SOCK_STREAM)

      handle <- poller.registerFileDescriptor(fd, true, false).mapK(LiftIO.liftK)
      serverGate <- FdGate.resource[F]
      _ <- Resource.eval {
        F.delay(fds.bind(fd, address)) *> F.delay(fds.listen(fd, backlog.getOrElse(SoMaxConn)))
      }

      address0 = address
      info = new SocketInfo[F] {
        def getOption[A](key: SocketOption.Key[A]) =
          F.delay(serverGate(fds.getOption(fd, c.AF_UNIX, key)))
        def setOption[A](key: SocketOption.Key[A], value: A) =
          F.delay(serverGate(fds.setOption(fd, c.AF_UNIX, key, value)))
        def supportedOptions = F.pure(fds.supportedOptions(datagram = false, c.AF_UNIX))
        val address = address0
      }

      clients = Stream
        .resource {
          // errors (e.g. out of descriptors, or an unsupported option) end the stream, like NIO's
          accepted[F](fds, fd, handle, serverGate)
            .flatMap { clientFd =>
              Resource.eval(fds.setAcceptedOptionsF(clientFd, c.AF_UNIX, filteredOptions)).flatMap {
                case false => Resource.pure[F, Option[Socket[F]]](None) // the client has gone
                case true  =>
                  for {
                    handle <- poller.registerFileDescriptor(clientFd, true, true).mapK(LiftIO.liftK)
                    socket <- FdPollingSocket[F](
                      fds,
                      clientFd,
                      c.AF_UNIX,
                      handle,
                      address,
                      UnixSocketAddress("")
                    )
                  } yield Some(socket)
              }
            }
        }
        .repeat
        .unNone

    } yield ServerSocket(info, clients)).flatMap(server => deleteOnClose.map(_ => server))
  }
}
