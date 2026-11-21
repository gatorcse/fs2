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

import cats.effect.IO
import cats.syntax.all._
import com.comcast.ip4s._

import scala.concurrent.duration._

/** Behaviour specific to the poller-based sockets, run on Scala Native and on the JVM with
  * `fs2-io-ffm` (where NIO behaves differently, e.g. it has no abstract Unix names).
  */
abstract class FdPollingSuite extends Fs2Suite {

  private val isLinux = System.getProperty("os.name", "").toLowerCase.startsWith("linux")

  private def tcpPair =
    Network[IO]
      .bind(SocketAddress(ip"127.0.0.1", Port.Wildcard))
      .flatMap { server =>
        Network[IO]
          .connect(server.address)
          .product(server.accept.head.compile.resource.lastOrError)
      }

  test("concurrent writes don't interleave") {
    val size = 1 << 20
    val a = Chunk.array(Array.fill[Byte](size)(1))
    val b = Chunk.array(Array.fill[Byte](size)(2))
    tcpPair.use { case (client, accepted) =>
      val writes = (client.write(a), client.write(b)).parTupled >> client.endOfOutput
      val reads = accepted.reads.compile.to(Array)
      (writes, reads).parMapN { (_, bytes) =>
        assertEquals(bytes.length, 2 * size)
        val runs = (1 until bytes.length).count(i => bytes(i) != bytes(i - 1)) + 1
        assertEquals(runs, 2)
      }
    }
  }

  test("zero-length reads return an empty chunk") {
    tcpPair.use { case (client, _) =>
      (client.read(0), client.readN(0)).tupled.map { case (read, readN) =>
        assertEquals(read, Some(Chunk.empty[Byte]))
        assertEquals(readN, Chunk.empty[Byte])
      }
    }
  }

  test("operations after close fail with ClosedChannelException") {
    tcpPair.allocated.flatMap { case ((client, _), release) =>
      release >>
        client.write(Chunk.singleton(1.toByte)).intercept[ClosedChannelException] >>
        client.read(1).intercept[ClosedChannelException] >>
        client.getOption(SocketOption.NoDelay).intercept[ClosedChannelException].void
    }
  }

  test("every supported option of an IP socket can be read") {
    tcpPair.use { case (client, _) =>
      client.supportedOptions.flatMap(_.toList.traverse_(k => client.getOption(k)))
    }
  }

  private def unixAddress(name: String) =
    UnixSocketAddress(s"fs2-fd-polling-$name-${System.nanoTime()}.sock")

  test("every supported option of a Unix socket can be read") {
    val address = unixAddress("options")
    Network[IO]
      .bind(address)
      .flatMap(_ => Network[IO].connect(address))
      .use { socket =>
        socket.supportedOptions.flatMap { options =>
          IO(assert(!options.contains(SocketOption.NoDelay))) >>
            options.toList.traverse_(k => socket.getOption(k))
        }
      }
  }

  test("a Unix connect fails, rather than half-connecting, when the backlog is full") {
    val address = unixAddress("backlog")
    Network[IO]
      .bind(address, List(SocketOption.backlog(1)))
      .flatMap { _ =>
        // nothing accepts, so the backlog fills up
        List.fill(16)(Network[IO].connect(address).attempt).sequence
      }
      .use { results =>
        IO(assert(results.exists(_.isLeft), "every connect succeeded")) >>
          results.traverse_ {
            case Right(socket) => socket.write(Chunk.singleton(1.toByte))
            case Left(_)       => IO.unit
          }
      }
      .timeout(10.seconds)
  }

  test("abstract Unix socket names (Linux)") {
    assume(isLinux, "abstract names are Linux-only")
    val address = UnixSocketAddress(s"\u0000fs2-fd-polling-${System.nanoTime()}")
    val msg = Chunk.array("hello".getBytes)
    Network[IO]
      .bind(address)
      .flatMap { server =>
        assertEquals(server.address, address)
        Network[IO].connect(address).product(server.accept.head.compile.resource.lastOrError)
      }
      .use { case (client, accepted) =>
        client.write(msg) >> client.endOfOutput >> accepted.reads.compile
          .to(Chunk)
          .assertEquals(msg)
      }
  }

  test("a wildcard server accepts IPv4 clients and reports IPv4 peers") {
    Network[IO]
      .bind(SocketAddress(ip"0.0.0.0", Port.Wildcard))
      .flatMap { server =>
        Network[IO]
          .connect(SocketAddress(ip"127.0.0.1", server.address.asIpUnsafe.port))
          .product(server.accept.head.compile.resource.lastOrError)
      }
      .use { case (client, accepted) =>
        IO {
          assertEquals(accepted.peerAddress, client.address)
          assertEquals(accepted.peerAddress.asIpUnsafe.host, ip"127.0.0.1": IpAddress)
        }
      }
  }

  test("a wildcard datagram socket sends to and receives from IPv4 peers") {
    val msg = Chunk.array("ping".getBytes)
    (
      Network[IO].bindDatagramSocket(SocketAddress(ip"0.0.0.0", Port.Wildcard)),
      Network[IO].bindDatagramSocket(SocketAddress(ip"127.0.0.1", Port.Wildcard))
    ).tupled.use { case (wildcard, peer) =>
      val wildcardPort = wildcard.address.asIpUnsafe.port
      for {
        _ <- wildcard.write(Datagram(peer.address.asIpUnsafe, msg))
        received <- peer.read
        _ = assertEquals(received.remote, SocketAddress(ip"127.0.0.1": IpAddress, wildcardPort))
        _ <- peer.write(Datagram(received.remote, msg))
        reply <- wildcard.read
      } yield assertEquals(reply.remote, peer.address.asIpUnsafe)
    }.timeout(10.seconds)
  }

  test("a connected datagram socket can write to its peer's address") {
    val msg = Chunk.array("ping".getBytes)
    (
      Network[IO].bindDatagramSocket(SocketAddress(ip"127.0.0.1", Port.Wildcard)),
      Network[IO].bindDatagramSocket(SocketAddress(ip"127.0.0.1", Port.Wildcard))
    ).tupled.use { case (sender, peer) =>
      sender.connect(peer.address) >>
        sender.write(Datagram(peer.address.asIpUnsafe, msg)) >>
        peer.read.map(d => assertEquals(d.bytes, msg))
    }.timeout(10.seconds)
  }
}
