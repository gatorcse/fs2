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
package ffm

import cats.effect.IO
import cats.effect.unsafe.IORuntime
import cats.syntax.all._
import com.comcast.ip4s._
import fs2.io.net._
import fs2.io.net.tls.TLSSocketSuite
import fs2.io.net.udp.UdpSuite

class FfmSocketSuite extends SocketSuite {
  override def munitIORuntime: IORuntime = FfmRuntime.runtime

  test("Network uses poller-based sockets on a FileDescriptorPoller runtime") {
    Network[IO]
      .bind(SocketAddress(ip"127.0.0.1", Port.Wildcard))
      .flatMap { server =>
        Network[IO]
          .connect(server.address)
          .product(server.accept.take(1).compile.resource.lastOrError)
      }
      .use { sockets =>
        IO {
          assertEquals(sockets._1.getClass.getSimpleName, "FdPollingSocket")
          assertEquals(sockets._2.getClass.getSimpleName, "FdPollingSocket")
        }
      }
  }
}

class FfmUnixSocketsSuite extends UnixSocketsSuite {
  override def munitIORuntime: IORuntime = FfmRuntime.runtime
  override protected def networkSocketClassName = "FdPollingSocket"
}

class FfmUdpSuite extends UdpSuite {
  override def munitIORuntime: IORuntime = FfmRuntime.runtime
  protected override val network = Network[IO]

  test("Network uses poller-based datagram sockets on a FileDescriptorPoller runtime") {
    Network[IO].bindDatagramSocket().use { socket =>
      IO(assertEquals(socket.getClass.getSimpleName, "FdPollingDatagramSocket"))
    }
  }
}

class FfmTLSSocketSuite extends TLSSocketSuite {
  override def munitIORuntime: IORuntime = FfmRuntime.runtime
}
