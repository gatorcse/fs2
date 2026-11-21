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
package io.net

import cats.effect.kernel.{Resource, Sync}

import java.nio.channels.ClosedChannelException
import java.util.concurrent.atomic.AtomicInteger

/** Guards a socket's descriptor against use after close.
  *
  * Every system call on the descriptor runs inside [[apply]]. Once [[close]] has been called,
  * new calls fail with `ClosedChannelException`, and `close` returns only after the calls already
  * running have finished. The descriptor is closed after that, so no call can reach a closed
  * descriptor, or a new socket that reused its number.
  *
  * The calls are non-blocking, so `close` waits for a few microseconds at most.
  */
private[net] final class FdGate {
  // the sign bit: closed; the rest: calls running
  private[this] val state = new AtomicInteger(0)

  def isOpen: Boolean = state.get() >= 0

  def apply[A](call: => A): A = {
    enter()
    try call
    finally { state.decrementAndGet(); () }
  }

  private[this] def enter(): Unit = {
    var s = state.get()
    while (true) {
      if (s < 0) throw new ClosedChannelException
      if (state.compareAndSet(s, s + 1)) return
      s = state.get()
    }
  }

  def close(): Unit = {
    var s = state.get()
    while (s >= 0 && !state.compareAndSet(s, s | Int.MinValue)) s = state.get()
    while ((state.get() & Int.MaxValue) != 0) Thread.`yield`()
  }
}

private[net] object FdGate {

  /** A gate that closes when the resource is released. Acquire it after the descriptor's own
    * resource, so that it closes first.
    */
  def resource[F[_]](implicit F: Sync[F]): Resource[F, FdGate] =
    Resource.make(F.delay(new FdGate))(g => F.delay(g.close()))
}
