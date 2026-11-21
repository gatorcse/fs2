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

package fs2.buffer

/** The last `size` buffers a source handed out.
  *
  * A source leases a buffer with [[lease]], fills it, and then either [[push]]es it, which
  * releases the buffer pushed `size` pushes ago, or [[discard]]s it at end of input. So at
  * most `size` pushed buffers are live and each stays valid for `size - 1` further pushes,
  * and the last ones stay valid until [[close]]. This is [[NativeChunk]]'s rolling window.
  * A buffer between `lease` and `push` is owned by the window too, so cancellation then
  * leaks nothing.
  *
  * Owned by one stream, whose steps run one at a time (possibly on different threads, with
  * the fiber handoff ordering them), so it allocates from its own [[Magazine]] without
  * locks. Every buffer goes back through that magazine.
  */
private[buffer] final class LeaseWindow(magazine: Magazine, size: Int) {
  require(size >= 1, s"window size must be at least 1, was $size")

  private[this] val ring = new Array[NativeBuf](size)
  private[this] var next = 0
  private[this] var pending: NativeBuf = null
  private[this] var closed = false

  /** Leases a buffer of `capacity` bytes to fill. Releases nothing yet. */
  def lease(capacity: Int): NativeBuf = {
    if (closed) throw new IllegalStateException("window is closed")
    if (pending ne null) throw new IllegalStateException("a leased buffer is still pending")
    pending = magazine.allocate(capacity)
    pending
  }

  /** Adds the pending buffer to the window, releasing the oldest if the window is full. */
  def push(): Unit = {
    val old = ring(next)
    ring(next) = pending
    pending = null
    next = (next + 1) % size
    if (old ne null) magazine.release(old)
  }

  /** Releases the pending buffer, which was not used. */
  def discard(): Unit = {
    val b = pending
    pending = null
    if (b ne null) magazine.release(b)
  }

  /** Releases the pending buffer and every buffer in the window, then closes the magazine. */
  def close(): Unit =
    if (!closed) {
      closed = true
      discard()
      var i = 0
      while (i < size) {
        val b = ring(i)
        if (b ne null) {
          ring(i) = null
          magazine.release(b)
        }
        i += 1
      }
      magazine.close()
    }
}
