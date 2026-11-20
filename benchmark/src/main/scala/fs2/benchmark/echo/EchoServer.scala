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
package benchmark
package echo

import cats.effect.{IO, IOApp}
import com.comcast.ip4s._
import fs2.io.net.{Network, SocketOption}

import java.lang.management.ManagementFactory

/** TCP echo server used as the phase 0 baseline for the FFM polling spike.
  *
  * Plain `socket.reads.through(socket.writes)` per connection on the default cats-effect runtime
  * (`SelectorSystem`), i.e. what an fs2 user gets today.
  */
object EchoServer extends IOApp.Simple {

  private val port = Integer.getInteger("echo.port", 9000).intValue

  def run: IO[Unit] =
    IO(startAllocReporter()) >>
      IO.println(s"fs2 echo port=$port") >>
      Network[IO]
        .bindAndAccept(
          SocketAddress(ip"0.0.0.0", Port.fromInt(port).get),
          List(SocketOption.noDelay(true))
        )
        .map(socket => socket.reads.through(socket.writes).handleErrorWith(_ => Stream.empty))
        .parJoinUnbounded
        .compile
        .drain

  /** Prints `STAT <epochMillis> <allocatedBytes> <gcCount> <gcMillis>` to stderr every second. */
  private def startAllocReporter(): Unit = {
    val tmx = ManagementFactory.getThreadMXBean
    // JDK 21+ API; looked up reflectively because this project compiles with --release 8
    val totalAllocated =
      classOf[com.sun.management.ThreadMXBean].getMethod("getTotalThreadAllocatedBytes")
    val t = new Thread(
      () =>
        while (true) {
          var gcCount, gcMs = 0L
          ManagementFactory.getGarbageCollectorMXBeans.forEach { gc =>
            gcCount += math.max(0L, gc.getCollectionCount)
            gcMs += math.max(0L, gc.getCollectionTime)
          }
          System.err.println(
            s"STAT ${System.currentTimeMillis()} ${totalAllocated.invoke(tmx)} $gcCount $gcMs"
          )
          Thread.sleep(1000)
        },
      "alloc-reporter"
    )
    t.setDaemon(true)
    t.start()
  }
}
