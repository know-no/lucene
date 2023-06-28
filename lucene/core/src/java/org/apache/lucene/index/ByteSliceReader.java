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
package org.apache.lucene.index;

import java.io.IOException;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.ByteBlockPool;
//参考： https://juejin.cn/post/7140233960415559710#heading-0
/* IndexInput that knows how to read the byte slices written
 * by Posting and PostingVector.  We read the bytes in
 * each slice until we hit the end of that slice at which
 * point we read the forwarding address of the next slice
 * and then jump to it.*/  //本质是slice的链表，把slice中的数据读取出来。
final class ByteSliceReader extends DataInput { // 当index之后，我们在内存里构建了倒排等数据结构，在flush阶段，读取内存中的倒排数据持久化成索引文件。
  ByteBlockPool pool; //倒排数据的两个stream中分别是由多个slice组成的两个链表，ByteSliceReader是用来读取slice的。slice的读取最重要的是判断当前slice的结束位置以及找到下一个slice的位置。ByteSliceReader用endIndex记录了stream的结束位置，用limit记录当前slice的结束位置，因此当limit不等于endIndex的时候，就说明还存在下一个slice，并且下一个slice的地址就是limit后面四个字节的值，而limit的确定是由ByteBlockPool.LEVEL_SIZE_ARRAY根据level计算得到的。
  int bufferUpto; // pool,即slice所在的byteBlockPool，往往是在内存里构建索引的时候用的那个。 bufferUpTo，pool中的哪个buffer
  byte[] buffer; // 当前在读取buffer
  public int upto; //
  int limit;//当前slice的结束位置
  int level;//当前读取到的slice level
  public int bufferOffset; // buffer在pool中的起始位置的start

  public int endIndex; // 这个数据源的slice链链表的最后一个slice的结束位置的下一个位置.  endIndex-1是最后一个byte的位置

  public void init(ByteBlockPool pool, int startIndex, int endIndex) {

    assert endIndex - startIndex >= 0;
    assert startIndex >= 0;
    assert endIndex >= 0;

    this.pool = pool;
    this.endIndex = endIndex;

    level = 0;
    bufferUpto = startIndex / ByteBlockPool.BYTE_BLOCK_SIZE;
    bufferOffset = bufferUpto * ByteBlockPool.BYTE_BLOCK_SIZE;
    buffer = pool.buffers[bufferUpto];
    upto = startIndex & ByteBlockPool.BYTE_BLOCK_MASK;

    final int firstSize = ByteBlockPool.LEVEL_SIZE_ARRAY[0];

    if (startIndex + firstSize >= endIndex) {
      // There is only this one slice to read
      limit = endIndex & ByteBlockPool.BYTE_BLOCK_MASK;
    } else limit = upto + firstSize - 4;
  }

  public boolean eof() {
    assert upto + bufferOffset <= endIndex;
    return upto + bufferOffset == endIndex; // endIndex
  }

  @Override
  public byte readByte() {
    assert !eof();
    assert upto <= limit;
    if (upto == limit) nextSlice();
    return buffer[upto++];
  }

  public long writeTo(DataOutput out) throws IOException {
    long size = 0;
    while (true) {
      if (limit + bufferOffset == endIndex) {
        assert endIndex - bufferOffset >= upto;
        out.writeBytes(buffer, upto, limit - upto);
        size += limit - upto;
        break;
      } else {
        out.writeBytes(buffer, upto, limit - upto);
        size += limit - upto;
        nextSlice();
      }
    }

    return size;
  }

  public void nextSlice() {

    // Skip to our next slice
    final int nextIndex = (int) BitUtil.VH_LE_INT.get(buffer, limit);

    level = ByteBlockPool.NEXT_LEVEL_ARRAY[level];
    final int newSize = ByteBlockPool.LEVEL_SIZE_ARRAY[level];

    bufferUpto = nextIndex / ByteBlockPool.BYTE_BLOCK_SIZE;
    bufferOffset = bufferUpto * ByteBlockPool.BYTE_BLOCK_SIZE;

    buffer = pool.buffers[bufferUpto];
    upto = nextIndex & ByteBlockPool.BYTE_BLOCK_MASK;

    if (nextIndex + newSize >= endIndex) {
      // We are advancing to the final slice
      assert endIndex - nextIndex > 0;
      limit = endIndex - bufferOffset;
    } else {
      // This is not the final slice (subtract 4 for the
      // forwarding address at the end of this new slice)
      limit = upto + newSize - 4;
    }
  }

  @Override
  public void readBytes(byte[] b, int offset, int len) {
    while (len > 0) {
      final int numLeft = limit - upto;
      if (numLeft < len) {
        // Read entire slice
        System.arraycopy(buffer, upto, b, offset, numLeft);
        offset += numLeft;
        len -= numLeft;
        nextSlice();
      } else {
        // This slice is the last one
        System.arraycopy(buffer, upto, b, offset, len);
        upto += len;
        break;
      }
    }
  }

  @Override
  public void skipBytes(long numBytes) {
    if (numBytes < 0) {
      throw new IllegalArgumentException("numBytes must be >= 0, got " + numBytes);
    }
    while (numBytes > 0) {
      final int numLeft = limit - upto;
      if (numLeft < numBytes) {
        numBytes -= numLeft;
        nextSlice();
      } else {
        upto += numBytes;
        break;
      }
    }
  }
}
