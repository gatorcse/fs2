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

package fs2.buffer;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;

/**
 * Accessors with the value layouts held in static finals, so the JIT treats them as
 * constants and compiles each access to a plain load or store. Scala object fields are
 * not trusted as constants, which would leave every access going through a var handle.
 */
final class SegmentAccess {
  private SegmentAccess() {}

  static final ValueLayout.OfByte BYTE = ValueLayout.JAVA_BYTE;
  static final ValueLayout.OfShort SHORT_BE =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
  static final ValueLayout.OfShort SHORT_LE =
      ValueLayout.JAVA_SHORT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  static final ValueLayout.OfInt INT_BE =
      ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
  static final ValueLayout.OfInt INT_LE =
      ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
  static final ValueLayout.OfLong LONG_BE =
      ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.BIG_ENDIAN);
  static final ValueLayout.OfLong LONG_LE =
      ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

  static byte getByte(MemorySegment s, long i) { return s.get(BYTE, i); }
  static void putByte(MemorySegment s, long i, byte v) { s.set(BYTE, i, v); }

  static short getShortBE(MemorySegment s, long i) { return s.get(SHORT_BE, i); }
  static short getShortLE(MemorySegment s, long i) { return s.get(SHORT_LE, i); }
  static void putShortBE(MemorySegment s, long i, short v) { s.set(SHORT_BE, i, v); }
  static void putShortLE(MemorySegment s, long i, short v) { s.set(SHORT_LE, i, v); }

  static int getIntBE(MemorySegment s, long i) { return s.get(INT_BE, i); }
  static int getIntLE(MemorySegment s, long i) { return s.get(INT_LE, i); }
  static void putIntBE(MemorySegment s, long i, int v) { s.set(INT_BE, i, v); }
  static void putIntLE(MemorySegment s, long i, int v) { s.set(INT_LE, i, v); }

  static long getLongBE(MemorySegment s, long i) { return s.get(LONG_BE, i); }
  static long getLongLE(MemorySegment s, long i) { return s.get(LONG_LE, i); }
  static void putLongBE(MemorySegment s, long i, long v) { s.set(LONG_BE, i, v); }
  static void putLongLE(MemorySegment s, long i, long v) { s.set(LONG_LE, i, v); }

  static void copyToArray(MemorySegment s, long offset, byte[] dst, int dstIndex, int length) {
    MemorySegment.copy(s, BYTE, offset, dst, dstIndex, length);
  }

  static void copyFromArray(byte[] src, int srcIndex, MemorySegment s, long offset, int length) {
    MemorySegment.copy(src, srcIndex, s, BYTE, offset, length);
  }
}
