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
package org.apache.lucene.util.packed;

import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.store.DataOutput;

// Packs high order byte first, to match
// IndexOutput.writeInt/Long/Short byte order

final class PackedWriter extends PackedInts.Writer {

  boolean finished;
  final PackedInts.Format format;
  final BulkOperation encoder;
  final byte[] nextBlocks; // 我们知道BulkOperationPacked在编码压缩数据的时候,把bits放入block中, 但是这里writer的block不是一个概念
  final long[] nextValues;//nextBlocks中的block ,是 writer 把数据拆分成段, 避免过大,并且PackedWriter对每段数据使用一样的 encoder,即相同的bitsPerValue
  final int iterations; // 而 AbstractBlockPackedWriter , 在拆段的同时,每个段,还可以用不同 bitsPerValue, 之所以这样, 是因为:
  int off;              // PackedWriter适用于所有数据量大小差不多的情况，如果数据中存在少量值比较大的，则会影响压缩效果，因为所有value是按最大的值来计算bitsPerValue的。
  int written;

  PackedWriter( // PackedWriter是已知所要处理的数据的bitsPerValue，根据这个bitsPerValue获取对应的编码器，因此所有的数据都使用一样的编码器。
      PackedInts.Format format, DataOutput out, int valueCount, int bitsPerValue, int mem) {
    super(out, valueCount, bitsPerValue);
    this.format = format;
    encoder = BulkOperation.of(format, bitsPerValue);
    iterations = encoder.computeIterations(valueCount, mem);
    nextBlocks = new byte[iterations * encoder.byteBlockCount()];
    nextValues = new long[iterations * encoder.byteValueCount()];
    off = 0;
    written = 0;
    finished = false;
  }

  @Override
  protected PackedInts.Format getFormat() {
    return format;
  }

  @Override
  public void add(long v) throws IOException {
    assert PackedInts.unsignedBitsRequired(v) <= bitsPerValue;
    assert !finished;
    if (valueCount != -1 && written >= valueCount) {
      throw new EOFException("Writing past end of stream");
    }
    nextValues[off++] = v;
    if (off == nextValues.length) {
      flush();
    }
    ++written;
  }

  @Override
  public void finish() throws IOException {
    assert !finished;
    if (valueCount != -1) {
      while (written < valueCount) {
        add(0L);
      }
    }
    flush();
    finished = true;
  }

  private void flush() throws IOException {
    encoder.encode(nextValues, 0, nextBlocks, 0, iterations);
    final int blockCount = (int) format.byteCount(PackedInts.VERSION_CURRENT, off, bitsPerValue);
    out.writeBytes(nextBlocks, blockCount);
    Arrays.fill(nextValues, 0L);
    off = 0;
  }

  @Override
  public int ord() {
    return written - 1;
  }
}
