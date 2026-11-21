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

import java.util.ServiceConfigurationError
import java.util.ServiceLoader

/** Finds the JVM's [[SocketSyscalls]], if any: `fs2-io-ffm` provides one through
  * `java.util.ServiceLoader`. Without it, or with `-Dfs2.io.net.fdPolling=false`, the JVM uses
  * the NIO socket providers.
  */
private[fs2] object SocketSyscallsLoader {

  lazy val sockets: Option[FdSockets] =
    if (System.getProperty("fs2.io.net.fdPolling", "true").equalsIgnoreCase("false")) None
    else load().map(new FdSockets(_))

  private def load(): Option[SocketSyscalls] =
    try {
      val it = ServiceLoader.load(classOf[SocketSyscalls], getClass.getClassLoader).iterator()
      var found: Option[SocketSyscalls] = None
      while (found.isEmpty && hasNext(it))
        try found = Some(it.next())
        catch {
          // the provider is present but can't be used here (e.g. an older JDK)
          case _: ServiceConfigurationError | _: LinkageError => ()
        }
      found
    } catch {
      case _: ServiceConfigurationError | _: LinkageError | _: SecurityException => None
    }

  private def hasNext(it: java.util.Iterator[?]): Boolean =
    try it.hasNext
    catch { case _: ServiceConfigurationError | _: LinkageError => false }
}
