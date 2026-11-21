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

package fs2.io.ffm.internal;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.StructLayout;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_BYTE;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;

/**
 * Socket system calls through the Foreign Function &amp; Memory API (JDK 25+), for fs2's
 * poller-based sockets.
 *
 * <p>Written in Java so that every {@code MethodHandle.invokeExact} call site has the exact type
 * the JIT needs to inline the downcall. Every method returns what the C function returns, or
 * {@code -errno} on failure; {@code errno} is captured right after the call into a per-thread
 * segment.
 *
 * <p>Heap arrays are passed to the kernel without a copy: every call that takes one is
 * non-blocking and bound with {@code Linker.Option.critical(true)}, which pins the array for the
 * duration of the call. Calls that can block are never critical, since a critical call holds up
 * safepoints: {@code close} (with {@code SO_LINGER}); {@code bind} and {@code connect}, which
 * look up or create a file for a Unix socket; and {@code getsockopt} and {@code setsockopt}, which
 * can take sleeping kernel locks (multicast memberships take the routing lock on Linux). Those
 * copy their arguments off-heap instead.
 */
public final class SocketCalls {

  private SocketCalls() {}

  public static final String OS = System.getProperty("os.name", "").toLowerCase();
  public static final String ARCH = System.getProperty("os.arch", "").toLowerCase();
  public static final boolean IS_LINUX = OS.startsWith("linux");
  public static final boolean IS_MAC = OS.startsWith("mac") || OS.startsWith("darwin");

  // --- linker plumbing -------------------------------------------------------------------------

  private static final Linker LINKER = Linker.nativeLinker();

  private static final StructLayout CAPTURE_LAYOUT = Linker.Option.captureStateLayout();
  private static final VarHandle ERRNO =
      CAPTURE_LAYOUT.varHandle(MemoryLayout.PathElement.groupElement("errno"));

  private static final Linker.Option CAPTURE = Linker.Option.captureCallState("errno");
  private static final Linker.Option CRITICAL = Linker.Option.critical(false);
  private static final Linker.Option CRITICAL_HEAP = Linker.Option.critical(true);

  private static final ThreadLocal<MemorySegment> CAPTURE_TL =
      ThreadLocal.withInitial(() -> Arena.ofAuto().allocate(CAPTURE_LAYOUT));

  private static MethodHandle bind(String name, FunctionDescriptor fd, Linker.Option... opts) {
    MemorySegment addr =
        LINKER
            .defaultLookup()
            .find(name)
            .orElseThrow(() -> new UnsatisfiedLinkError("libc symbol not found: " + name));
    return LINKER.downcallHandle(addr, fd, opts);
  }

  private static int ret(MemorySegment cap, int r) {
    return r < 0 ? -(int) ERRNO.get(cap, 0L) : r;
  }

  private static int ret(MemorySegment cap, long r) {
    return r < 0 ? -(int) ERRNO.get(cap, 0L) : (int) r;
  }

  private static MemorySegment heap(byte[] a, int offset, int length) {
    return a == null ? MemorySegment.NULL : MemorySegment.ofArray(a).asSlice(offset, length);
  }

  private static MemorySegment offHeap(Arena arena, byte[] a) {
    if (a == null) return MemorySegment.NULL;
    MemorySegment seg = arena.allocate(Math.max(a.length, 1));
    MemorySegment.copy(a, 0, seg, JAVA_BYTE, 0, a.length);
    return seg;
  }

  private static MemorySegment heap(byte[] a) {
    return a == null ? MemorySegment.NULL : MemorySegment.ofArray(a);
  }

  private static MemorySegment heap(int[] a) {
    return a == null ? MemorySegment.NULL : MemorySegment.ofArray(a);
  }

  private static RuntimeException rethrow(Throwable t) {
    if (t instanceof RuntimeException) return (RuntimeException) t;
    if (t instanceof Error) throw (Error) t;
    return new RuntimeException(t);
  }

  // --- handles ---------------------------------------------------------------------------------

  private static final MethodHandle SOCKET =
      bind("socket", FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT), CAPTURE);

  private static final MethodHandle FCNTL =
      bind(
          "fcntl",
          FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT),
          Linker.Option.firstVariadicArg(2),
          CAPTURE);

  private static final MethodHandle CLOSE =
      bind("close", FunctionDescriptor.of(JAVA_INT, JAVA_INT), CAPTURE);

  private static final FunctionDescriptor I_IPI =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT);
  private static final FunctionDescriptor I_IPP =
      FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, ADDRESS);

  private static final MethodHandle BIND = bind("bind", I_IPI, CAPTURE);
  private static final MethodHandle CONNECT = bind("connect", I_IPI, CAPTURE);
  private static final MethodHandle LISTEN =
      bind("listen", FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT), CRITICAL, CAPTURE);

  /** {@code accept4} on Linux; {@code accept} elsewhere (no flags). */
  private static final MethodHandle ACCEPT =
      IS_LINUX
          ? bind(
              "accept4",
              FunctionDescriptor.of(JAVA_INT, JAVA_INT, ADDRESS, ADDRESS, JAVA_INT),
              CRITICAL,
              CAPTURE)
          : bind("accept", I_IPP, CRITICAL, CAPTURE);

  private static final MethodHandle GETSOCKNAME =
      bind("getsockname", I_IPP, CRITICAL_HEAP, CAPTURE);
  private static final MethodHandle GETPEERNAME =
      bind("getpeername", I_IPP, CRITICAL_HEAP, CAPTURE);

  private static final MethodHandle GETSOCKOPT =
      bind(
          "getsockopt",
          FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS, ADDRESS),
          CAPTURE);
  private static final MethodHandle SETSOCKOPT =
      bind(
          "setsockopt",
          FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT, JAVA_INT, ADDRESS, JAVA_INT),
          CAPTURE);

  private static final MethodHandle READ =
      bind(
          "read",
          FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG),
          CRITICAL_HEAP,
          CAPTURE);
  private static final MethodHandle WRITE =
      bind(
          "write",
          FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG),
          CRITICAL_HEAP,
          CAPTURE);
  private static final MethodHandle SEND =
      bind(
          "send",
          FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT),
          CRITICAL_HEAP,
          CAPTURE);
  private static final MethodHandle RECVFROM =
      bind(
          "recvfrom",
          FunctionDescriptor.of(JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT, ADDRESS, ADDRESS),
          CRITICAL_HEAP,
          CAPTURE);
  private static final MethodHandle SENDTO =
      bind(
          "sendto",
          FunctionDescriptor.of(
              JAVA_LONG, JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT, ADDRESS, JAVA_INT),
          CRITICAL_HEAP,
          CAPTURE);
  private static final MethodHandle SHUTDOWN =
      bind("shutdown", FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_INT), CRITICAL, CAPTURE);

  private static final MethodHandle STRERROR =
      bind("strerror", FunctionDescriptor.of(ADDRESS, JAVA_INT));

  // --- calls -----------------------------------------------------------------------------------

  public static int socket(int domain, int type, int protocol) {
    MemorySegment cap = CAPTURE_TL.get();
    try {
      return ret(cap, (int) SOCKET.invokeExact(cap, domain, type, protocol));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int fcntl(int fd, int cmd, int arg) {
    MemorySegment cap = CAPTURE_TL.get();
    try {
      return ret(cap, (int) FCNTL.invokeExact(cap, fd, cmd, arg));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int close(int fd) {
    MemorySegment cap = CAPTURE_TL.get();
    try {
      return ret(cap, (int) CLOSE.invokeExact(cap, fd));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int bind(int fd, byte[] addr, int addrLen) {
    MemorySegment cap = CAPTURE_TL.get();
    try (Arena arena = Arena.ofConfined()) {
      return ret(cap, (int) BIND.invokeExact(cap, fd, offHeap(arena, addr), addrLen));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int listen(int fd, int backlog) {
    MemorySegment cap = CAPTURE_TL.get();
    try {
      return ret(cap, (int) LISTEN.invokeExact(cap, fd, backlog));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int connect(int fd, byte[] addr, int addrLen) {
    MemorySegment cap = CAPTURE_TL.get();
    try (Arena arena = Arena.ofConfined()) {
      return ret(cap, (int) CONNECT.invokeExact(cap, fd, offHeap(arena, addr), addrLen));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  /** {@code accept4(fd, NULL, NULL, flags)} on Linux, else {@code accept(fd, NULL, NULL)}. */
  public static int accept(int fd, int flags) {
    MemorySegment cap = CAPTURE_TL.get();
    try {
      if (IS_LINUX)
        return ret(
            cap,
            (int) ACCEPT.invokeExact(cap, fd, MemorySegment.NULL, MemorySegment.NULL, flags));
      else
        return ret(cap, (int) ACCEPT.invokeExact(cap, fd, MemorySegment.NULL, MemorySegment.NULL));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int getsockname(int fd, byte[] addr, int[] addrLen) {
    MemorySegment cap = CAPTURE_TL.get();
    try {
      return ret(cap, (int) GETSOCKNAME.invokeExact(cap, fd, heap(addr), heap(addrLen)));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int getpeername(int fd, byte[] addr, int[] addrLen) {
    MemorySegment cap = CAPTURE_TL.get();
    try {
      return ret(cap, (int) GETPEERNAME.invokeExact(cap, fd, heap(addr), heap(addrLen)));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int getsockopt(int fd, int level, int name, byte[] value, int[] valueLen) {
    MemorySegment cap = CAPTURE_TL.get();
    try (Arena arena = Arena.ofConfined()) {
      MemorySegment v = offHeap(arena, value);
      MemorySegment len = arena.allocate(JAVA_INT);
      len.set(JAVA_INT, 0, valueLen[0]);
      int r = ret(cap, (int) GETSOCKOPT.invokeExact(cap, fd, level, name, v, len));
      if (r >= 0) {
        int n = Math.min(len.get(JAVA_INT, 0), value.length);
        MemorySegment.copy(v, JAVA_BYTE, 0, value, 0, n);
        valueLen[0] = n;
      }
      return r;
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int setsockopt(int fd, int level, int name, byte[] value, int valueLen) {
    MemorySegment cap = CAPTURE_TL.get();
    try (Arena arena = Arena.ofConfined()) {
      return ret(
          cap, (int) SETSOCKOPT.invokeExact(cap, fd, level, name, offHeap(arena, value), valueLen));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int read(int fd, byte[] buf, int offset, int length) {
    MemorySegment cap = CAPTURE_TL.get();
    try {
      return ret(
          cap, (long) READ.invokeExact(cap, fd, heap(buf, offset, length), (long) length));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  /** {@code send(MSG_NOSIGNAL)} on Linux; {@code write} elsewhere ({@code SO_NOSIGPIPE}). */
  public static int write(int fd, byte[] buf, int offset, int length, int msgNoSignal) {
    MemorySegment cap = CAPTURE_TL.get();
    try {
      if (IS_LINUX)
        return ret(
            cap,
            (long)
                SEND.invokeExact(
                    cap, fd, heap(buf, offset, length), (long) length, msgNoSignal));
      else
        return ret(
            cap, (long) WRITE.invokeExact(cap, fd, heap(buf, offset, length), (long) length));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int recvfrom(
      int fd, byte[] buf, int offset, int length, int flags, byte[] addr, int[] addrLen) {
    MemorySegment cap = CAPTURE_TL.get();
    try {
      return ret(
          cap,
          (long)
              RECVFROM.invokeExact(
                  cap,
                  fd,
                  heap(buf, offset, length),
                  (long) length,
                  flags,
                  heap(addr),
                  heap(addrLen)));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int sendto(
      int fd, byte[] buf, int offset, int length, int flags, byte[] addr, int addrLen) {
    MemorySegment cap = CAPTURE_TL.get();
    try {
      return ret(
          cap,
          (long)
              SENDTO.invokeExact(
                  cap, fd, heap(buf, offset, length), (long) length, flags, heap(addr), addrLen));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static int shutdown(int fd, int how) {
    MemorySegment cap = CAPTURE_TL.get();
    try {
      return ret(cap, (int) SHUTDOWN.invokeExact(cap, fd, how));
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }

  public static String strerror(int errno) {
    try {
      MemorySegment msg = (MemorySegment) STRERROR.invokeExact(errno);
      if (msg.equals(MemorySegment.NULL)) return "errno " + errno;
      return msg.reinterpret(1024).getString(0, StandardCharsets.UTF_8);
    } catch (Throwable t) {
      throw rethrow(t);
    }
  }
}
