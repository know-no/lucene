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
package org.apache.lucene.codecs.lucene90.compressing;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexInput;

class StoredFieldsInts {

  private static final int BLOCK_SIZE = 128;
  private static final int BLOCK_SIZE_MINUS_ONE = BLOCK_SIZE - 1;

  private StoredFieldsInts() {}

  static void writeInts(int[] values, int start, int count, DataOutput out) throws IOException {
    boolean allEqual = true;
    for (int i = 1; i < count; ++i) { // 判断是否全等
      if (values[start + i] != values[start]) {
        allEqual = false;
        break;
      }
    }
    if (allEqual) { // 全等的话，就没啥好说的了， 这也是最普遍的情况，所以上面的判断还是很有必要的.
      out.writeByte((byte) 0);
      out.writeVInt(values[0]);
    } else {
      long max = 0;
      for (int i = 0; i < count; ++i) {
        max |= Integer.toUnsignedLong(values[start + i]); // 不仅是为了找最大值
      } // max |= some
      if (max <= 0xff) { // 这种间隙压缩有什么好处
        out.writeByte((byte) 8); // 因为最大值 255 = 2^8 -1 ， 所以写的时候，以8个bit，也就是一个字节为单位写?
        writeInts8(out, count, values, start);
      } else if (max <= 0xffff) { // 2^16 - 1
        out.writeByte((byte) 16);
        writeInts16(out, count, values, start);
      } else {
        out.writeByte((byte) 32);
        writeInts32(out, count, values, start);
      }
    }
  }

  private static void writeInts8(DataOutput out, int count, int[] values, int offset)
      throws IOException {
    int k = 0; // count - 127; k+= 128
    for (; k < count - BLOCK_SIZE_MINUS_ONE; k += BLOCK_SIZE) {
      int step = offset + k;   // 一批128个long，
      for (int i = 0; i < 16; ++i) { // 16*8=128, 一次是128个数字写入，每个数字的大小是8位，一个字节， 所以是128个字节
        long l = // 从values里 等距离取出8个，每个距离是16，取16次。16 * 8 = 128,所以共取出了128
            ((long) values[step + i] << 56) // 128个，拆分成16个long， 每个long里是8个数字
                | ((long) values[step + 16 + i] << 48)
                | ((long) values[step + 32 + i] << 40)
                | ((long) values[step + 48 + i] << 32)
                | ((long) values[step + 64 + i] << 24)
                | ((long) values[step + 80 + i] << 16)
                | ((long) values[step + 96 + i] << 8)
                | (long) values[step + 112 + i];
        out.writeLong(l);
      }
    }
    for (; k < count; k++) {
      out.writeByte((byte) values[offset + k]);
    }
  }

  private static void writeInts16(DataOutput out, int count, int[] values, int offset)
      throws IOException {
    int k = 0;
    for (; k < count - BLOCK_SIZE_MINUS_ONE; k += BLOCK_SIZE) {
      int step = offset + k;
      for (int i = 0; i < 32; ++i) {// 写入block， 每次取四个，取32次， 一共取出128个. 128个拆分成了32个long
        long l = // |---------32个空隙-------|---------32个空隙-------|--------32个空隙-------|---------32个空隙-------
            ((long) values[step + i] << 48)
                | ((long) values[step + 32 + i] << 32)
                | ((long) values[step + 64 + i] << 16)
                | (long) values[step + 96 + i];
        out.writeLong(l);
      }
    }
    for (; k < count; k++) {
      out.writeShort((short) values[offset + k]);
    }
  }

  private static void writeInts32(DataOutput out, int count, int[] values, int offset)
      throws IOException {
    int k = 0;
    for (; k < count - BLOCK_SIZE_MINUS_ONE; k += BLOCK_SIZE) {
      int step = offset + k; // 所以step 为128
      for (int i = 0; i < 64; ++i) { // 64*2个 128个 32位的int // 写入一个block中去, 以long的方式写入，拆分成了64个long
        long l = ((long) values[step + i] << 32) | (long) values[step + 64 + i];
        out.writeLong(l);
      }
    }
    for (; k < count; k++) {
      out.writeInt(values[offset + k]);// 剩余不满128个数量， 以int的方式写入
    }
  }

  /** Read {@code count} integers into {@code values}. */
  static void readInts(IndexInput in, int count, long[] values, int offset) throws IOException {
    final int bpv = in.readByte();
    switch (bpv) {
      case 0:
        Arrays.fill(values, offset, offset + count, in.readVInt());
        break;
      case 8:
        readInts8(in, count, values, offset);
        break;
      case 16:
        readInts16(in, count, values, offset);
        break;
      case 32:
        readInts32(in, count, values, offset);
        break;
      default:
        throw new IOException("Unsupported number of bits per value: " + bpv);
    }
  }

  private static void readInts8(IndexInput in, int count, long[] values, int offset)
      throws IOException {
    int k = 0;
    for (; k < count - BLOCK_SIZE_MINUS_ONE; k += BLOCK_SIZE) {
      final int step = offset + k;
      in.readLongs(values, step, 16);
      for (int i = 0; i < 16; ++i) {
        final long l = values[step + i];
        values[step + i] = (l >>> 56) & 0xFFL;
        values[step + 16 + i] = (l >>> 48) & 0xFFL;
        values[step + 32 + i] = (l >>> 40) & 0xFFL;
        values[step + 48 + i] = (l >>> 32) & 0xFFL;
        values[step + 64 + i] = (l >>> 24) & 0xFFL;
        values[step + 80 + i] = (l >>> 16) & 0xFFL;
        values[step + 96 + i] = (l >>> 8) & 0xFFL;
        values[step + 112 + i] = l & 0xFFL;
      }
    }
    for (; k < count; k++) {
      values[offset + k] = Byte.toUnsignedInt(in.readByte());
    }
  }

  private static void readInts16(IndexInput in, int count, long[] values, int offset)
      throws IOException {
    int k = 0;
    for (; k < count - BLOCK_SIZE_MINUS_ONE; k += BLOCK_SIZE) {
      int step = offset + k;
      in.readLongs(values, step, 32);
      for (int i = 0; i < 32; ++i) {
        final long l = values[step + i];
        values[step + i] = (l >>> 48) & 0xFFFFL;
        values[step + 32 + i] = (l >>> 32) & 0xFFFFL;
        values[step + 64 + i] = (l >>> 16) & 0xFFFFL;
        values[step + 96 + i] = l & 0xFFFFL;
      }
    }
    for (; k < count; k++) {
      values[offset + k] = Short.toUnsignedInt(in.readShort());
    }
  }

  private static void readInts32(IndexInput in, int count, long[] values, int offset)
      throws IOException {
    int k = 0;
    for (; k < count - BLOCK_SIZE_MINUS_ONE; k += BLOCK_SIZE) {
      final int step = offset + k;
      in.readLongs(values, step, 64);
      for (int i = 0; i < 64; ++i) {
        final long l = values[step + i];
        values[step + i] = l >>> 32;
        values[step + 64 + i] = l & 0xFFFFFFFFL;
      }
    }
    for (; k < count; k++) {
      values[offset + k] = in.readInt();
    }
  }
}
