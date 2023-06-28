/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.store;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import org.apache.lucene.util.BitUtil;

/**
 * Abstract base class for performing read operations of Lucene's low-level data types.
 *
 * <p>{@code DataInput} may only be used from one thread, because it is not thread safe (it keeps
 * internal state like file position). To allow multithreaded use, every {@code DataInput} instance
 * must be cloned before used in another thread. Subclasses must therefore implement {@link
 * #clone()}, returning a new {@code DataInput} which operates on the same underlying resource, but
 * positioned independently.
 */
public abstract class DataInput implements Cloneable {

  /**
   * Reads and returns a single byte.
   *
   * @see DataOutput#writeByte(byte)
   */
  public abstract byte readByte() throws IOException;

  /**
   * Reads a specified number of bytes into an array at the specified offset.
   *
   * @param b the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param len the number of bytes to read
   * @see DataOutput#writeBytes(byte[],int)
   */
  public abstract void readBytes(byte[] b, int offset, int len) throws IOException;

  /**
   * Reads a specified number of bytes into an array at the specified offset with control over
   * whether the read should be buffered (callers who have their own buffer should pass in "false"
   * for useBuffer). Currently only {@link BufferedIndexInput} respects this parameter.
   *
   * @param b the array to read bytes into
   * @param offset the offset in the array to start storing bytes
   * @param len the number of bytes to read
   * @param useBuffer set to false if the caller will handle buffering.
   * @see DataOutput#writeBytes(byte[],int)
   */
  public void readBytes(byte[] b, int offset, int len, boolean useBuffer) throws IOException {
    // Default to ignoring useBuffer entirely
    readBytes(b, offset, len);
  }

  /**
   * Reads two bytes and returns a short (LE byte order).
   *
   * @see DataOutput#writeShort(short)
   * @see BitUtil#VH_LE_SHORT
   */
  public short readShort() throws IOException {
    final byte b1 = readByte();
    final byte b2 = readByte();
    return (short) (((b2 & 0xFF) << 8) | (b1 & 0xFF)); // b1还是低位, 说明存在底层是小端存储
  }

  /**
   * Reads four bytes and returns an int (LE byte order).
   *
   * @see DataOutput#writeInt(int)
   * @see BitUtil#VH_LE_INT
   */
  public int readInt() throws IOException {
    final byte b1 = readByte();
    final byte b2 = readByte();
    final byte b3 = readByte();
    final byte b4 = readByte(); // 其实没必要和0xFF做&运算吧
    return ((b4 & 0xFF) << 24) | ((b3 & 0xFF) << 16) | ((b2 & 0xFF) << 8) | (b1 & 0xFF);
  }

  /**
   * Reads an int stored in variable-length format. Reads between one and five bytes. Smaller values
   * take fewer bytes. Negative numbers are supported, but should be avoided.
   *
   * <p>The format is described further in {@link DataOutput#writeVInt(int)}.
   *
   * @see DataOutput#writeVInt(int)
   */
  public int readVInt() throws IOException {
    /* This is the original code of this method,
     * but a Hotspot bug (see LUCENE-2975) corrupts the for-loop if
     * readByte() is inlined. So the loop was unwinded!
    byte b = readByte();
    int i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = readByte();
      i |= (b & 0x7F) << shift;
    }
    return i;
    */  // lucene 以1作为连续位, 即需要继续读取, 如果是0 , 则表示最后一个了.
    byte b = readByte();
    if (b >= 0) return b;
    int i = b & 0x7F; // i 是后七位
    b = readByte();
    i |= (b & 0x7F) << 7; // 新都出来的放到bytes的左侧, 向左移动7位, i放在后面
    if (b >= 0) return i; // 如果是最后一个字节了, 直接返回
    b = readByte();
    i |= (b & 0x7F) << 14;
    if (b >= 0) return i;
    b = readByte();
    i |= (b & 0x7F) << 21;
    if (b >= 0) return i; // 一共 28 bit
    b = readByte();
    // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
    i |= (b & 0x0F) << 28; // 0x0F => 0000 1111  再取四位
    if ((b & 0xF0) == 0) return i; // 如果这个字节的高四位不是0000, 说明发生了问题
    throw new IOException("Invalid vInt detected (too many bits)");
  } // 从这个读取过程还可以看出, 因为java使用的是小端, 即数据高位在地址高位, 所以越往后read的byte越是低位数据,向左放.

  /**
   * Read a {@link BitUtil#zigZagDecode(int) zig-zag}-encoded {@link #readVInt() variable-length}
   * integer.
   *
   * @see DataOutput#writeZInt(int)
   */
  public int readZInt() throws IOException { // 读取一个之前被zigZag编码/压缩的int
    return BitUtil.zigZagDecode(readVInt());
  }

  /**
   * Reads eight bytes and returns a long (LE byte order).
   *
   * @see DataOutput#writeLong(long)
   * @see BitUtil#VH_LE_LONG
   */
  public long readLong() throws IOException { // java里数据低位和地址低位是从左边, 其实做都无所谓, 只是 << 符号向高位推进
    return (readInt() & 0xFFFFFFFFL) | (((long) readInt()) << 32); // 后读的是数据高位, 在地址高位, << 向左推进.
  }

  /**
   * Read a specified number of longs.
   *
   * @lucene.experimental
   */
  public void readLongs(long[] dst, int offset, int length) throws IOException {
    Objects.checkFromIndexSize(offset, length, dst.length);
    for (int i = 0; i < length; ++i) {
      dst[offset + i] = readLong();
    }
  }

  /**
   * Reads a specified number of ints into an array at the specified offset.
   *
   * @param dst the array to read bytes into
   * @param offset the offset in the array to start storing ints
   * @param length the number of ints to read
   */
  public void readInts(int[] dst, int offset, int length) throws IOException {
    Objects.checkFromIndexSize(offset, length, dst.length);
    for (int i = 0; i < length; ++i) {
      dst[offset + i] = readInt();
    }
  }

  /**
   * Reads a specified number of floats into an array at the specified offset.
   *
   * @param floats the array to read bytes into
   * @param offset the offset in the array to start storing floats
   * @param len the number of floats to read
   */
  public void readFloats(float[] floats, int offset, int len) throws IOException {
    Objects.checkFromIndexSize(offset, len, floats.length);
    for (int i = 0; i < len; i++) {
      floats[offset + i] = Float.intBitsToFloat(readInt()); // 单精度浮点数也是用int存储的
    }
  }

  /**
   * Reads a long stored in variable-length format. Reads between one and nine bytes. Smaller values
   * take fewer bytes. Negative numbers are not supported.
   *
   * <p>The format is described further in {@link DataOutput#writeVInt(int)}.
   *
   * @see DataOutput#writeVLong(long)
   */
  public long readVLong() throws IOException {
    return readVLong(false);
  }

  private long readVLong(boolean allowNegative) throws IOException {
    /* This is the original code of this method,
     * but a Hotspot bug (see LUCENE-2975) corrupts the for-loop if
     * readByte() is inlined. So the loop was unwinded!
    byte b = readByte();
    long i = b & 0x7F;
    for (int shift = 7; (b & 0x80) != 0; shift += 7) {
      b = readByte();
      i |= (b & 0x7FL) << shift;
    }
    return i;
    */
    byte b = readByte(); // 还是和之前 VInt 一样的压缩方式
    if (b >= 0) return b; // 1 是延续位, 如果b>=0, 说明最高位是0, 不是1, 不延续, 直接返回.
    long i = b & 0x7FL; // 取低数据位7位
    b = readByte();
    i |= (b & 0x7FL) << 7; // 将新读到的数据任然取低数据位7位, 然后像高数据位推进7, 与之前的低位数据合并.
    if (b >= 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 14;
    if (b >= 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 21;
    if (b >= 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 28;
    if (b >= 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 35;
    if (b >= 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 42;
    if (b >= 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 49;
    if (b >= 0) return i;
    b = readByte();
    i |= (b & 0x7FL) << 56; // 加上 b&0x7F 的七位, 已经 63位了
    if (b >= 0) return i;
    if (allowNegative) { // 如果方法调用允许负数, 则继续读一个byte, 8位.
      b = readByte();
      i |= (b & 0x7FL) << 63;   // b 取低7位, 然后合并. 因为是最后一个byte了, 只能是用来标识正负数的 0/1
      if (b == 0 || b == 1) return i;
      throw new IOException("Invalid vLong detected (more than 64 bits)");
    } else { // b < 0, 最高位为1, 延续位, 说明这可能是个负数数据, 也可能是比 正long 大的.
      throw new IOException("Invalid vLong detected (negative values disallowed)");
    }
  }

  /**
   * Read a {@link BitUtil#zigZagDecode(long) zig-zag}-encoded {@link #readVLong() variable-length}
   * integer. Reads between one and ten bytes.
   *
   * @see DataOutput#writeZLong(long)
   */
  public long readZLong() throws IOException {
    return BitUtil.zigZagDecode(readVLong(true));
  }

  /**
   * Reads a string.
   *
   * @see DataOutput#writeString(String)
   */
  public String readString() throws IOException {
    int length = readVInt();
    final byte[] bytes = new byte[length];
    readBytes(bytes, 0, length);
    return new String(bytes, 0, length, StandardCharsets.UTF_8); // 说明是先转化为utf-8编码,然后才写到底层的
  }

  /**
   * Returns a clone of this stream.
   *
   * <p>Clones of a stream access the same data, and are positioned at the same point as the stream
   * they were cloned from.
   *
   * <p>Expert: Subclasses must ensure that clones may be positioned at different points in the
   * input from each other and from the stream they were cloned from.
   */
  @Override
  public DataInput clone() {
    try {
      return (DataInput) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new Error("This cannot happen: Failing to clone DataInput", e);
    }
  }

  /**
   * Reads a Map&lt;String,String&gt; previously written with {@link
   * DataOutput#writeMapOfStrings(Map)}.
   *
   * @return An immutable map containing the written contents.
   */
  public Map<String, String> readMapOfStrings() throws IOException {
    int count = readVInt();
    if (count == 0) {
      return Collections.emptyMap();
    } else if (count == 1) {
      return Collections.singletonMap(readString(), readString());
    } else {
      Map<String, String> map = count > 10 ? new HashMap<>() : new TreeMap<>();
      for (int i = 0; i < count; i++) {
        final String key = readString();
        final String val = readString();
        map.put(key, val);
      }
      return Collections.unmodifiableMap(map);
    }
  }

  /**
   * Reads a Set&lt;String&gt; previously written with {@link DataOutput#writeSetOfStrings(Set)}.
   *
   * @return An immutable set containing the written contents.
   */
  public Set<String> readSetOfStrings() throws IOException {
    int count = readVInt();
    if (count == 0) {
      return Collections.emptySet();
    } else if (count == 1) {
      return Collections.singleton(readString());
    } else {
      Set<String> set = count > 10 ? new HashSet<>() : new TreeSet<>();
      for (int i = 0; i < count; i++) {
        set.add(readString());
      }
      return Collections.unmodifiableSet(set);
    }
  }

  /**
   * Skip over <code>numBytes</code> bytes. This method may skip bytes in whatever way is most
   * optimal, and may not have the same behavior as reading the skipped bytes. In general, negative
   * <code>numBytes</code> are not supported.
   */
  public abstract void skipBytes(final long numBytes) throws IOException;
}
