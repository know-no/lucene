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

import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;

/**
 * DataInput backed by a byte array. <b>WARNING:</b> This class omits all low-level checks.
 *
 * @lucene.experimental
 */
public final class ByteArrayDataInput extends DataInput {

  private byte[] bytes; // 仅仅包一个数组, 数据从这个数组来, 可以省略许多低级的检查
// 什么场景用的多呢? 数据已知已有,已经在内存中的情况.仅仅包装一下子, 作为datainput,方便使用接口使用
  private int pos;
  private int limit;

  public ByteArrayDataInput(byte[] bytes) {
    reset(bytes);
  }

  public ByteArrayDataInput(byte[] bytes, int offset, int len) {
    reset(bytes, offset, len);
  }

  public ByteArrayDataInput() {
    reset(BytesRef.EMPTY_BYTES);
  }

  public void reset(byte[] bytes) {
    reset(bytes, 0, bytes.length);
  }

  // NOTE: sets pos to 0, which is not right if you had
  // called reset w/ non-zero offset!!
  public void rewind() {
    pos = 0;
  }

  public int getPosition() {
    return pos;
  }

  public void setPosition(int pos) {
    this.pos = pos;
  }

  public void reset(byte[] bytes, int offset, int len) {
    this.bytes = bytes;
    pos = offset;
    limit = offset + len;
  }

  public int length() {
    return limit;
  }

  public boolean eof() {
    return pos == limit;
  }

  @Override
  public void skipBytes(long count) {
    pos += count;
  }

  @Override
  public short readShort() {
    try {
      return (short) BitUtil.VH_LE_SHORT.get(bytes, pos);
    } finally {
      pos += Short.BYTES;
    }
  }

  @Override
  public int readInt() {
    try {
      return (int) BitUtil.VH_LE_INT.get(bytes, pos);
    } finally {
      pos += Integer.BYTES;
    }
  }

  @Override
  public long readLong() {
    try {
      return (long) BitUtil.VH_LE_LONG.get(bytes, pos);
    } finally {
      pos += Long.BYTES;
    }
  }

  @Override
  public int readVInt() { // 没必要重写父类的此方法. 反正正确实现了 readByte，这里吧readByte的实现拿出来贴到这里，是为了更好的内联？？
    byte b = bytes[pos++];
    if (b >= 0) return b;
    int i = b & 0x7F;
    b = bytes[pos++];
    i |= (b & 0x7F) << 7;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7F) << 14;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7F) << 21;
    if (b >= 0) return i;
    b = bytes[pos++];
    // Warning: the next ands use 0x0F / 0xF0 - beware copy/paste errors:
    i |= (b & 0x0F) << 28;
    if ((b & 0xF0) == 0) return i;
    throw new RuntimeException("Invalid vInt detected (too many bits)");
  }

  @Override
  public long readVLong() {
    byte b = bytes[pos++];
    if (b >= 0) return b;
    long i = b & 0x7FL;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 7;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 14;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 21;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 28;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 35;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 42;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 49;
    if (b >= 0) return i;
    b = bytes[pos++];
    i |= (b & 0x7FL) << 56;
    if (b >= 0) return i;
    throw new RuntimeException("Invalid vLong detected (negative values disallowed)");
  }

  // NOTE: AIOOBE not EOF if you read too much
  @Override
  public byte readByte() {
    return bytes[pos++];
  }

  // NOTE: AIOOBE not EOF if you read too much
  @Override
  public void readBytes(byte[] b, int offset, int len) {
    System.arraycopy(bytes, pos, b, offset, len);
    pos += len;
  }
}
