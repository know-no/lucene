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
package org.apache.lucene.codecs.lucene90;

import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.DIRECT_MONOTONIC_BLOCK_SHIFT;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.NUMERIC_BLOCK_SHIFT;
import static org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat.NUMERIC_BLOCK_SIZE;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.EmptyDocValuesProducer;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.SortedSetSelector;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.LongsRef;
import org.apache.lucene.util.MathUtil;
import org.apache.lucene.util.StringHelper;
import org.apache.lucene.util.compress.LZ4;
import org.apache.lucene.util.packed.DirectMonotonicWriter;
import org.apache.lucene.util.packed.DirectWriter;

/** writer for {@link Lucene90DocValuesFormat} */
final class Lucene90DocValuesConsumer extends DocValuesConsumer {

  IndexOutput data, meta;
  final int maxDoc;
  private byte[] termsDictBuffer; // termsDictBuffer 用来构建 TermsDict 的中间数据结构

  /** expert: Creates a new writer */
  public Lucene90DocValuesConsumer(
      SegmentWriteState state,
      String dataCodec,
      String dataExtension,
      String metaCodec,
      String metaExtension)
      throws IOException {
    this.termsDictBuffer = new byte[1 << 14];
    boolean success = false;
    try {
      String dataName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, dataExtension);
      data = state.directory.createOutput(dataName, state.context);
      CodecUtil.writeIndexHeader(
          data,
          dataCodec,
          Lucene90DocValuesFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      String metaName =
          IndexFileNames.segmentFileName(
              state.segmentInfo.name, state.segmentSuffix, metaExtension);
      meta = state.directory.createOutput(metaName, state.context);
      CodecUtil.writeIndexHeader(
          meta,
          metaCodec,
          Lucene90DocValuesFormat.VERSION_CURRENT,
          state.segmentInfo.getId(),
          state.segmentSuffix);
      maxDoc = state.segmentInfo.maxDoc();
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(this);
      }
    }
  }

  @Override
  public void close() throws IOException {
    boolean success = false;
    try {
      if (meta != null) {
        meta.writeInt(-1); // write EOF marker
        CodecUtil.writeFooter(meta); // write checksum
      }
      if (data != null) {
        CodecUtil.writeFooter(data); // write checksum
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(data, meta);
      } else {
        IOUtils.closeWhileHandlingException(data, meta);
      }
      meta = data = null;
    }
  }

  @Override
  public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene90DocValuesFormat.NUMERIC);

    writeValues( // 包装成 sortedset 在写
        field,
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            return DocValues.singleton(valuesProducer.getNumeric(field)); // 每次get都会产生一个新的
          }
        },
        false);
  }

  private static class MinMaxTracker {
    long min, max, numValues, spaceInBits;

    MinMaxTracker() {
      reset();
      spaceInBits = 0;
    }

    private void reset() {
      min = Long.MAX_VALUE;
      max = Long.MIN_VALUE;
      numValues = 0;
    }

    /** Accumulate a new value. */
    void update(long v) {
      min = Math.min(min, v);
      max = Math.max(max, v);
      ++numValues;
    }

    /** Accumulate state from another tracker. */
    void update(MinMaxTracker other) {
      min = Math.min(min, other.min);
      max = Math.max(max, other.max);
      numValues += other.numValues;
    }

    /** Update the required space. */
    void finish() { // 看看存下最大的差值, 要多少个bit ,且会round到内置的 固定比特数
      if (max > min) {
        spaceInBits += DirectWriter.unsignedBitsRequired(max - min) * numValues;
      }
    }

    /** Update space usage and get ready for accumulating values for the next block. */
    void nextBlock() {
      finish();
      reset();
    }
  }
  // 这个ords参数, 为true, 说明这里的valuesProducer的值本身就是映射后(词典)的值, 所以, 不用再用词典优化了
  private long[] writeValues(FieldInfo field, DocValuesProducer valuesProducer, boolean ords)
      throws IOException { // 产生一个新的BufferedNumericDocValues // 返回的数字, 根据参数ords来判断:是此doc的docvalue的次序ord,还是值本身
    SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
    final long firstValue; // 第一个值是什么, 特殊在哪里? 是最小的吗?不像啊, NumericDocValuesWriter在写入的时候没什么特别的呀
    if (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) { // 确实没什么特别的,不过 gcd的差值计算算法不用在乎它是不是最小
      firstValue = values.nextValue();
    } else {
      firstValue = 0L;
    }
    values = valuesProducer.getSortedNumeric(field); // 又产生一个新的BufferedNumericDocValues
    int numDocsWithValue = 0; // 对于 dvm, header -> DocsWithField -> NumberValues
    MinMaxTracker minMax = new MinMaxTracker();
    MinMaxTracker blockMinMax = new MinMaxTracker(); // 追踪最大最小, 块内的
    long gcd = 0; // 公约数
    Set<Long> uniqueValues = ords ? null : new HashSet<>(); // 独立个数小于 256 会有特别的优化
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (int i = 0, count = values.docValueCount(); i < count; ++i) {
        long v = values.nextValue();

        if (gcd != 1) {
          if (v < Long.MIN_VALUE / 2 || v > Long.MAX_VALUE / 2) { // 这种情况下, 不可能有除了1的公约数存在的
            // in that case v - minValue might overflow and make the GCD computation return
            // wrong results. Since these extreme values are unlikely, we just discard
            // GCD computation for them
            gcd = 1;
          } else {
            gcd = MathUtil.gcd(gcd, v - firstValue);
          }
        }

        blockMinMax.update(v); // 更新最大最小值
        if (blockMinMax.numValues == NUMERIC_BLOCK_SIZE) { // 个数到达可以形成一个块
          minMax.update(blockMinMax);
          blockMinMax.nextBlock(); // 分配新的块
        }
        // if 语句里的 顺序不能乱
        if (uniqueValues != null && uniqueValues.add(v) && uniqueValues.size() > 256) {
          uniqueValues = null;
        }
      }

      numDocsWithValue++;
    }

    minMax.update(blockMinMax);
    minMax.finish();
    blockMinMax.finish();

    if (ords && minMax.numValues > 0) {
      if (minMax.min != 0) {
        throw new IllegalStateException(
            "The min value for ordinals should always be 0, got " + minMax.min);
      }
      if (minMax.max != 0 && gcd != 1) { // ?如果不用, 可以不计算啊, 但为什么前面还计算?
        throw new IllegalStateException(  // 因为如果是 ords, 则 getValue获取的不是真实的值数字, 而是 映射后的编码序号 如100-> 0, 102->1
            "GCD compression should never be used on ordinals, found gcd=" + gcd);
      }
    }

    final long numValues = minMax.numValues;
    long min = minMax.min;
    final long max = minMax.max;
    assert blockMinMax.spaceInBits <= minMax.spaceInBits;
    // 分三种情况讨论doc是否有此field, 有的话, 有几个值
    if (numDocsWithValue == 0) { // meta[-2, 0]: No documents with values  3. 部分文档包含这个字段 Lucene会把包含这个字段的doc放入一个bitsets中， 然后将这个bitset写入到.dvd里即可， .dvm则负责记录在.dvd文件中的offset和length。
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else if (numDocsWithValue == maxDoc) { // meta[-1, 0]: All documents has values
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else { // meta[data.offset, data.length]: IndexedDISI structure for documents with values
      long offset = data.getFilePointer();
      meta.writeLong(offset); // docsWithFieldOffset // 先写offset到meta里
      values = valuesProducer.getSortedNumeric(field); // 再把value的情况, bitset 写入 .dvd 里, 然后再把length 写入meta
      final short jumpTableEntryCount = // 返回的是 jump table的block count, 可以用来读出jumps数组: https://www.codercto.com/a/49745.html/
          IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER); // https://www.amazingkoala.com.cn/Lucene/gongjulei/2020/0514/141.html
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount);
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER);
    }

    meta.writeLong(numValues); // 继续写meta,value的数量，在NumericDocValue的情况下，这个就是包含该field的文档的数量, 如果在SortedNumericDocValue的情况下，这个数值是指各文档内包含value数量的总和，这个数值用一个long类型来写入。
    final int numBitsPerValue; // 存储每个压缩过的docid 需要的最小bit个数
    boolean doBlocks = false;
    Map<Long, Integer> encode = null;
    if (min >= max) {    // meta[-1]: All values are 0 // 所有的 number docvalue 都是一样的, 直接写入 -1
      numBitsPerValue = 0;
      meta.writeInt(-1); // tablesize
    } else { //特殊优化: 词典编码
      if (uniqueValues != null // 独特的值不为空,说明 < 256个, 有特殊优化空间
          && uniqueValues.size() > 1 // 不是都一样 // 且: 可以给unique编号,编号连续,则每个编号占有的bit个数就很小, 在下于公约数方式的时候
          && DirectWriter.unsignedBitsRequired(uniqueValues.size() - 1)
              < DirectWriter.unsignedBitsRequired((max - min) / gcd)) { //  do 特殊优化
        numBitsPerValue = DirectWriter.unsignedBitsRequired(uniqueValues.size() - 1);
        final Long[] sortedUniqueValues = uniqueValues.toArray(new Long[0]);
        Arrays.sort(sortedUniqueValues);
        meta.writeInt(sortedUniqueValues.length); // tablesize
        for (Long v : sortedUniqueValues) { // 把映射的原值, 写到meta里, 不写到data里, data里写映射的序号
          meta.writeLong(v); // table[] entry
        }
        encode = new HashMap<>();
        for (int i = 0; i < sortedUniqueValues.length; ++i) {
          encode.put(sortedUniqueValues[i], i);
        }
        min = 0;
        gcd = 1;
      } else { // 第三种: 都不满足, 只有使用gcd的方式了. 压缩编码
        uniqueValues = null;
        // we do blocks if that appears to save 10+% storage
        doBlocks = // 粗略估计, 能剩下10%以上的存储
            minMax.spaceInBits > 0 && (double) blockMinMax.spaceInBits / minMax.spaceInBits <= 0.9;
        if (doBlocks) { // 如果能省下, 就 用blocks
          numBitsPerValue = 0xFF;
          meta.writeInt(-2 - NUMERIC_BLOCK_SHIFT); // tablesize // -16 标识 用 blocks
        } else {
          numBitsPerValue = DirectWriter.unsignedBitsRequired((max - min) / gcd);
          if (gcd == 1
              && min > 0
              && DirectWriter.unsignedBitsRequired(max)
                  == DirectWriter.unsignedBitsRequired(max - min)) {
            min = 0;
          }
          meta.writeInt(-1); // table size // -1 标识不用 blocks
        }
      }
    }

    meta.writeByte((byte) numBitsPerValue); // 写入  numBetsPerValue
    meta.writeLong(min);
    meta.writeLong(gcd);
    long startOffset = data.getFilePointer(); // 写入data 当前的offset, 然后在写入length
    meta.writeLong(startOffset); // valueOffset
    long jumpTableOffset = -1;
    if (doBlocks) {
      jumpTableOffset = writeValuesMultipleBlocks(valuesProducer.getSortedNumeric(field), gcd);
    } else if (numBitsPerValue != 0) {
      writeValuesSingleBlock(
          valuesProducer.getSortedNumeric(field), numValues, numBitsPerValue, min, gcd, encode);
    }
    meta.writeLong(data.getFilePointer() - startOffset); // valuesLength
    meta.writeLong(jumpTableOffset);
    return new long[] {numDocsWithValue, numValues};
  }

  private void writeValuesSingleBlock(
      SortedNumericDocValues values,
      long numValues,
      int numBitsPerValue,
      long min,
      long gcd,
      Map<Long, Integer> encode)
      throws IOException {
    DirectWriter writer = DirectWriter.getInstance(data, numValues, numBitsPerValue);
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (int i = 0, count = values.docValueCount(); i < count; ++i) {
        long v = values.nextValue();
        if (encode == null) {
          writer.add((v - min) / gcd);
        } else {
          writer.add(encode.get(v));
        }
      }
    }
    writer.finish();
  }

  // Returns the offset to the jump-table for vBPV
  private long writeValuesMultipleBlocks(SortedNumericDocValues values, long gcd)
      throws IOException {
    long[] offsets = new long[ArrayUtil.oversize(1, Long.BYTES)];
    int offsetsIndex = 0;
    final long[] buffer = new long[NUMERIC_BLOCK_SIZE];
    final ByteBuffersDataOutput encodeBuffer = ByteBuffersDataOutput.newResettableInstance();
    int upTo = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      for (int i = 0, count = values.docValueCount(); i < count; ++i) {
        buffer[upTo++] = values.nextValue();
        if (upTo == NUMERIC_BLOCK_SIZE) { // buffer 写满了, 接下里要写到 到data里去. flush之前, 记录写入的位置
          offsets = ArrayUtil.grow(offsets, offsetsIndex + 1);
          offsets[offsetsIndex++] = data.getFilePointer(); // 记录写入的位置
          writeBlock(buffer, NUMERIC_BLOCK_SIZE, gcd, encodeBuffer); // 会先把 buffer 写成block, 然后把block写入data
          upTo = 0;                                                  //  encodeBuffer 应该只是中间的临时存放工具
        }
      }
    }
    if (upTo > 0) {
      offsets = ArrayUtil.grow(offsets, offsetsIndex + 1);
      offsets[offsetsIndex++] = data.getFilePointer();
      writeBlock(buffer, upTo, gcd, encodeBuffer);
    }

    // All blocks has been written. Flush the offset jump-table
    final long offsetsOrigo = data.getFilePointer();
    for (int i = 0; i < offsetsIndex; i++) {
      data.writeLong(offsets[i]);
    }
    data.writeLong(offsetsOrigo);
    return offsetsOrigo;
  }

  private void writeBlock(long[] values, int length, long gcd, ByteBuffersDataOutput buffer)
      throws IOException { // values 里存放着 docvalue的原始值, 不过values只是其中的一小个数组
    assert length > 0;
    long min = values[0];
    long max = values[0];
    for (int i = 1; i < length; ++i) { // 再次找一下最大最小, 不过是找 values数组里的
      final long v = values[i];
      assert Math.floorMod(values[i] - min, gcd) == 0;
      min = Math.min(min, v);
      max = Math.max(max, v);
    }
    if (min == max) { // 这一批, 最大, 最小一样, 不扯淡了 data 里写0 后, 写min, 就是这个一样的值
      data.writeByte((byte) 0);
      data.writeLong(min);
    } else {
      final int bitsPerValue = DirectWriter.unsignedBitsRequired((max - min) / gcd);
      buffer.reset();
      assert buffer.size() == 0;
      final DirectWriter w = DirectWriter.getInstance(buffer, length, bitsPerValue);
      for (int i = 0; i < length; ++i) {
        w.add((values[i] - min) / gcd); // w 会把 values, 利用压缩编码, 编码成合适的大小, 放到block里, 再把block写入中间数组:buffer
      }
      w.finish();
      data.writeByte((byte) bitsPerValue);  // [(bitsPerValue | min | size | blocks), ...]
      data.writeLong(min);
      data.writeInt(Math.toIntExact(buffer.size()));
      buffer.copyTo(data); // 最后把中间数组复制进去data
    }
  }

  @Override
  public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number); // meta 写元数据 ,写field的global number 映射, 即这个field是哪个field, 因为每个段都有自己独立的fieldInfos
    meta.writeByte(Lucene90DocValuesFormat.BINARY); // meta , 写入数据的类型

    BinaryDocValues values = valuesProducer.getBinary(field); // 调用的是org.apache.lucene.index.EmptyDocValuesProducer.getBinary
    long start = data.getFilePointer(); // 获取 data 的文件的写入指针
    meta.writeLong(start); // dataOffset
    int numDocsWithField = 0;
    int minLength = Integer.MAX_VALUE;
    int maxLength = 0;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      numDocsWithField++;
      BytesRef v = values.binaryValue();
      int length = v.length;
      data.writeBytes(v.bytes, v.offset, v.length); // 非常干脆, 不压缩, 不去重, 直接写入
      minLength = Math.min(length, minLength);
      maxLength = Math.max(length, maxLength);
    }
    assert numDocsWithField <= maxDoc;
    meta.writeLong(data.getFilePointer() - start); // dataLength
    // meta 写入data length
    if (numDocsWithField == 0) { // 下面三个情况, 和 numeric docvalue的极其类似
      meta.writeLong(-2); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else if (numDocsWithField == maxDoc) {
      meta.writeLong(-1); // docsWithFieldOffset
      meta.writeLong(0L); // docsWithFieldLength
      meta.writeShort((short) -1); // jumpTableEntryCount
      meta.writeByte((byte) -1); // denseRankPower
    } else { // 只是部分doc 有这个field的docvalue
      long offset = data.getFilePointer();
      meta.writeLong(offset); // docsWithFieldOffset
      values = valuesProducer.getBinary(field); // 获得了全新的 docvalues
      final short jumpTableEntryCount = // 用 IndexedDISI 来写入, docid 的情况, docid是否存在,如果存在,则有docvalue
          IndexedDISI.writeBitSet(values, data, IndexedDISI.DEFAULT_DENSE_RANK_POWER);
      meta.writeLong(data.getFilePointer() - offset); // docsWithFieldLength
      meta.writeShort(jumpTableEntryCount); // 返回的是 jump table的 blockCount 的个数, 方便从 data 中读取  IndexedDIS
      meta.writeByte(IndexedDISI.DEFAULT_DENSE_RANK_POWER); //知道了 block count的个数c, c*2^16 得到data一共写入的INdexedDISI的byte个数
    }

    meta.writeInt(numDocsWithField); // 写入一共有多少个doc 有fields
    meta.writeInt(minLength);
    meta.writeInt(maxLength);
    if (maxLength > minLength) { // 即写入的docvalue长短不一
      start = data.getFilePointer(); // 接着刚才写入docvalue数据, 写入
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);

      final DirectMonotonicWriter writer =
          DirectMonotonicWriter.getInstance( // meta 数据还会写入meta中去, 如: DirectMonotonicWriter特有的avgInc, 还有min,
              meta, data, numDocsWithField + 1, DIRECT_MONOTONIC_BLOCK_SHIFT);
      long addr = 0;
      writer.add(addr);
      values = valuesProducer.getBinary(field);
      for (int doc = values.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = values.nextDoc()) {
        addr += values.binaryValue().length; // 以单调递增的压缩方式, 写入每个docvalue在data(termsvalue)中的结束位置. 结束位置n+1 - 结束位置n = 长度
        writer.add(addr);
      }
      writer.finish();
      meta.writeLong(data.getFilePointer() - start);
    }
  }

  @Override
  public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene90DocValuesFormat.SORTED);
    doAddSortedField(field, valuesProducer);
  }

  private void doAddSortedField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    writeValues( // 这里干了一件事: 就是将docId对应的term的ord写下来了. docId的顺序是0->n, 所以相当于写下了 doc -> term
        field,
        new EmptyDocValuesProducer() {
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            SortedDocValues sorted = valuesProducer.getSorted(field);
            NumericDocValues sortedOrds =
                new NumericDocValues() {
                  @Override
                  public long longValue() throws IOException {
                    return sorted.ordValue(); // 返回的是target doc的docvalue 顺序映射ord 吧
                  }

                  @Override
                  public boolean advanceExact(int target) throws IOException {
                    return sorted.advanceExact(target); // BufferedSorted 没实现这个
                  }

                  @Override
                  public int docID() {
                    return sorted.docID();
                  }

                  @Override
                  public int nextDoc() throws IOException {
                    return sorted.nextDoc();
                  }

                  @Override
                  public int advance(int target) throws IOException {
                    return sorted.advance(target);
                  }

                  @Override
                  public long cost() {
                    return sorted.cost();
                  }
                };
            return DocValues.singleton(sortedOrds);
          }
        },
        true);
    addTermsDict(DocValues.singleton(valuesProducer.getSorted(field)));// 构建 TermsDict
  }

  private void addTermsDict(SortedSetDocValues values) throws IOException {
    final long size = values.getValueCount(); // 共有多少的doc, 有docvalue
    meta.writeVLong(size); // # ValueCount

    int blockMask = Lucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_MASK; // 11 1111
    int shift = Lucene90DocValuesFormat.TERMS_DICT_BLOCK_LZ4_SHIFT; //  6
    // # DICT_BLOCK_SHIFT
    meta.writeInt(DIRECT_MONOTONIC_BLOCK_SHIFT);// monotonic shit -> 16 // 即想要以 2^16 位大小的数组作为一个数据分组
    ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
    ByteBuffersIndexOutput addressOutput =
        new ByteBuffersIndexOutput(addressBuffer, "temp", "temp");
    long numBlocks = (size + blockMask) >>> shift; // + blockMask 是为了保证 numblocks 至少为1 其实加一也行
    DirectMonotonicWriter writer =
        DirectMonotonicWriter.getInstance(
            meta, addressOutput, numBlocks, DIRECT_MONOTONIC_BLOCK_SHIFT);

    BytesRefBuilder previous = new BytesRefBuilder(); // 记录前一个处理的term, 方便计算公共前后缀
    long ord = 0;
    long start = data.getFilePointer();
    int maxLength = 0, maxBlockLength = 0;
    TermsEnum iterator = values.termsEnum(); // 获取 terms, 并且是按照ord的顺序获取的, 具体是因为包装成了SortedSetDocValuesTermsEnum
    // 且可以理解 docId 和 docValue的绑定,即成对出现,每次获取一个docId, 都会得到一个docValue, 即term
    LZ4.FastCompressionHashTable ht = new LZ4.FastCompressionHashTable();// 短时间记忆的hashtable
    ByteArrayDataOutput bufferedOutput = new ByteArrayDataOutput(termsDictBuffer);
    int dictLength = 0;

    for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
      if ((ord & blockMask) == 0) { // 即每次ord是 64(63+1)的倍数 , 就得到block的第一个value. block是TermDictBlock
        if (ord != 0) { // 判断ord是否等于0, 如果不等于0 , 说明ord, 已经超过了blockMask的范畴; 即已经将一个TermDictBlock写完了
          // flush the previous block       //ord在除了0之外,第一次 &blockMask=0的时候,是100 0000
          final int uncompressedLength =
              compressAndGetTermsDictBlockLength(bufferedOutput, dictLength, ht);
          maxBlockLength = Math.max(maxBlockLength, uncompressedLength);
          bufferedOutput.reset(termsDictBuffer);
        }
        // 写入 数据的长度; 即提交上一个分组
        writer.add(data.getFilePointer() - start); // 当ord是0的时候, data.getFilePointer()-start=0,
        // Write the first term both to the index output, and to the buffer where we'll use it as a
        // dictionary for compression // 完整存储 TermDictBlock的第一个值: length,  term本身
        data.writeVInt(term.length);
        data.writeBytes(term.bytes, term.offset, term.length); // 有写入数据本身
        bufferedOutput = maybeGrowBuffer(bufferedOutput, term.length); // 可能bufferedOutput的内置缓存 termsDictBuffer不够了,将它扩容
        bufferedOutput.writeBytes(term.bytes, term.offset, term.length); // 写入 bufferedOutput 是未来的为了方便计算 maxBlockLength
        dictLength = term.length;
      } else { // 比较前一个处理的 term 和 当前 term, 获得共同prefix的长度,
        final int prefixLength = StringHelper.bytesDifference(previous.get(), term);
        final int suffixLength = term.length - prefixLength;  // 剩余的后缀的长度
        assert suffixLength > 0; // terms are unique
        // Will write (suffixLength + 1 byte + 2 vint) bytes. Grow the buffer in need.
        bufferedOutput = maybeGrowBuffer(bufferedOutput, suffixLength + 11);
        bufferedOutput.writeByte(
            (byte) (Math.min(prefixLength, 15) | (Math.min(15, suffixLength - 1) << 4)));
        if (prefixLength >= 15) {
          bufferedOutput.writeVInt(prefixLength - 15);
        }
        if (suffixLength >= 16) {
          bufferedOutput.writeVInt(suffixLength - 16);
        } // 把剩余的suffix写入
        bufferedOutput.writeBytes(term.bytes, term.offset + prefixLength, suffixLength);
      }
      maxLength = Math.max(maxLength, term.length);
      previous.copyBytes(term);
      ++ord;
    }
    // Compress and write out the last block
    if (bufferedOutput.getPosition() > dictLength) { // 最后一个block, 提交了它
      final int uncompressedLength =
          compressAndGetTermsDictBlockLength(bufferedOutput, dictLength, ht);
      maxBlockLength = Math.max(maxBlockLength, uncompressedLength);
    }

    writer.finish(); // monotonic writer 结束
    meta.writeInt(maxLength); // 写入所有term的最大长度
    // Write one more int for storing max block length.
    meta.writeInt(maxBlockLength);
    meta.writeLong(start); // # 所有TermDictBlocks 的开始地址
    meta.writeLong(data.getFilePointer() - start); // # 所有TermDictBlocks 的长度
    start = data.getFilePointer();
    addressBuffer.copyTo(data); //刚才关于block的提交信息,都是写入的writer里(以addressBuffer为背后的存储支持),见580行, 现在将它写入真的data里
    meta.writeLong(start);                  // 即TermDictBlockAddress, 每个 TermDictBlock在 TermDictBlocks中的起始位置
    meta.writeLong(data.getFilePointer() - start);

    // Now write the reverse terms index
    writeTermsIndex(values);
  }

  private int compressAndGetTermsDictBlockLength(
      ByteArrayDataOutput bufferedOutput, int dictLength, LZ4.FastCompressionHashTable ht)
      throws IOException {
    int uncompressedLength = bufferedOutput.getPosition() - dictLength;
    data.writeVInt(uncompressedLength);
    LZ4.compressWithDictionary(termsDictBuffer, 0, dictLength, uncompressedLength, data, ht);
    return uncompressedLength;
  }

  private ByteArrayDataOutput maybeGrowBuffer(ByteArrayDataOutput bufferedOutput, int termLength) {
    int pos = bufferedOutput.getPosition(), originalLength = termsDictBuffer.length;
    if (pos + termLength >= originalLength - 1) {
      termsDictBuffer = ArrayUtil.grow(termsDictBuffer, originalLength + termLength);
      bufferedOutput = new ByteArrayDataOutput(termsDictBuffer, pos, termsDictBuffer.length - pos);
    }
    return bufferedOutput;
  }
  // 每处理1024个域值会生成一个PrefixValue
  private void writeTermsIndex(SortedSetDocValues values) throws IOException {
    final long size = values.getValueCount(); // # ValueCount
    meta.writeInt(Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT); // # Shift
    long start = data.getFilePointer();

    long numBlocks = // todo 这里为什么计算 numBlocks
        1L
            + ((size + Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) // mask : 1024 - 1 , 即10个1
                >>> Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_SHIFT);
    ByteBuffersDataOutput addressBuffer = new ByteBuffersDataOutput();
    DirectMonotonicWriter writer;
    try (ByteBuffersIndexOutput addressOutput =
        new ByteBuffersIndexOutput(addressBuffer, "temp", "temp")) {
      writer = // 把元信息写入 meta, 把实际的数据写入addressOutput, 待会复制到data
          DirectMonotonicWriter.getInstance(
              meta, addressOutput, numBlocks, DIRECT_MONOTONIC_BLOCK_SHIFT); // 2^16,
      TermsEnum iterator = values.termsEnum(); //
      BytesRefBuilder previous = new BytesRefBuilder(); // 每1024算一批, 每批的第一个
      long offset = 0;
      long ord = 0;
      for (BytesRef term = iterator.next(); term != null; term = iterator.next()) {
        if ((ord & Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) == 0) {  // ord是1024的倍数, 0, 1024, 2048
          writer.add(offset);                                                   // ord从0 开始, 所以, 即每一批的第一个
          final int sortKeyLength; // 和前一个块的最后一个term的最长公共前缀的长度 + 1
          if (ord == 0) { // 如果是ord 0, 则没有共同前缀, 且不加1 // 就是不写入的意思
            // no previous term: no bytes to write
            sortKeyLength = 0;
          } else {
            sortKeyLength = StringHelper.sortKeyLength(previous.get(), term);
          }
          offset += sortKeyLength;
          data.writeBytes(term.bytes, term.offset, sortKeyLength); // 写下
        } else if ((ord & Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK)
            == Lucene90DocValuesFormat.TERMS_DICT_REVERSE_INDEX_MASK) {// 11 1111 1111 == 11 1111 1111
          previous.copyBytes(term);                   // ord是从0开始的 刚满足一个块,即每个块的最后一个 复制到previous
        }
        ++ord;
      }  //sortKeyMeta就是prefixmeta,包含两个部分: Offset(可以找到这个sortkey在data里的位置), length(长度,读取到哪里)
      writer.add(offset);//TermsIndexMeta在meta里的形式是: 由MonotonicDirectWriter写入meta的min,avginc,length,SortKeyMeta,SortKeyValueMeta
      writer.finish();   //PrefixMeta里的offset,帮助在data里定位到了PrefixValue(就是上文679写入的),
      meta.writeLong(start);    // 在刚才的for循环中, 虽然写了很多数据, 但是往data里写的只有prefixvalue数组,连续写入.//与此同时,写入到meta的是Monotonic的索引
      meta.writeLong(data.getFilePointer() - start);// monotonic压数据的同时,还有自己的directwriter数据,但实际上在buffer里,
      start = data.getFilePointer();//688-689两行在meta写入的是prefixvalue数组在data里的始末位置,随后, 将buffer里压缩后offset写入data
      addressBuffer.copyTo(data);   // buffer里的offset写入data之后, 需要在meta中记录他们的始末位置
      meta.writeLong(start);
      meta.writeLong(data.getFilePointer() - start);
    }
  }

  @Override
  public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene90DocValuesFormat.SORTED_NUMERIC);
    doAddSortedNumericField(field, valuesProducer, false); // ords = false, 这里存储的是真实的数字, 不是映射过的ord
  }

  private void doAddSortedNumericField(// 关于存储的数字, 一个doc的docvalue数字不会进行排序,排序也不利于原样取回,
      FieldInfo field, DocValuesProducer valuesProducer, boolean ords) throws IOException {
    long[] stats = writeValues(field, valuesProducer, ords);
    int numDocsWithField = Math.toIntExact(stats[0]);
    long numValues = stats[1];
    assert numValues >= numDocsWithField;

    meta.writeInt(numDocsWithField);
    if (numValues > numDocsWithField) { // 说明有field有不止一个docvalue
      long start = data.getFilePointer();
      meta.writeLong(start);
      meta.writeVInt(DIRECT_MONOTONIC_BLOCK_SHIFT);
    // 这里还是用的MonotomicWriter,是因为MonotomicWriter存的不是docValue数组, 而不同doc之间的docvalues的边界,
      final DirectMonotonicWriter addressesWriter =// 如 [[1,3,2],[3,4],[4,5],[1,2,3]],边界是: 0 3 5 7 10
          DirectMonotonicWriter.getInstance(
              meta, data, numDocsWithField + 1L, DIRECT_MONOTONIC_BLOCK_SHIFT);
      long addr = 0;
      addressesWriter.add(addr);
      SortedNumericDocValues values = valuesProducer.getSortedNumeric(field);
      for (int doc = values.nextDoc();
          doc != DocIdSetIterator.NO_MORE_DOCS;
          doc = values.nextDoc()) {
        addr += values.docValueCount(); // 这里拿的是docValueCount, 然后写入addressWriter, 即上文中写入的是0, 3 5 7 10
        addressesWriter.add(addr);
      }
      addressesWriter.finish();
      meta.writeLong(data.getFilePointer() - start); //用monotic写完meta,和数据本身到 data后, 再把data的地址写入meta
    }   // 这样读取的时候, 先从meta中读取min,avginc,等等信息, 然后再读取数据的地址p, 从data的p位置,读取monotonic的写入的数据本身
  }

  private static boolean isSingleValued(SortedSetDocValues values) throws IOException {
    if (DocValues.unwrapSingleton(values) != null) {
      return true;
    }

    assert values.docID() == -1;
    for (int doc = values.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = values.nextDoc()) {
      int docValueCount = values.docValueCount();
      assert docValueCount > 0;
      if (docValueCount > 1) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer)
      throws IOException {
    meta.writeInt(field.number);
    meta.writeByte(Lucene90DocValuesFormat.SORTED_SET);
    //如果是单值,直接用单值的类操作就好,因为单值的话,doc如果有docValue
    if (isSingleValued(valuesProducer.getSortedSet(field))) {
      meta.writeByte((byte) 0); // multiValued (0 = singleValued)     // 则doc只有一个docvalue, 存储着的ord
      doAddSortedField(
          field,
          new EmptyDocValuesProducer() {
            @Override
            public SortedDocValues getSorted(FieldInfo field) throws IOException {
              return SortedSetSelector.wrap(
                  valuesProducer.getSortedSet(field), SortedSetSelector.Type.MIN);
            }
          });
      return;
    }
    meta.writeByte((byte) 1); // multiValued (1 = multiValued)// 存放的是 doc -> (term1, term2, term4) 即一个doc有几个term, 在单值里是对应的, 在单值里,通过docId,在docIdWithFields可以得到它在哪个IndexedDISI块里,
    //再根据块的元信息, 即这个块之前, 有多少个有field的doc存在, 最红得到 docCountsBefore + docCountInBlock, 然后在根据写入顺序得到的doc->ord存储里, 找到对应的ordid,
    doAddSortedNumericField(// ordId, 因为 termsDict是按照ordId的顺序存储 term的, TermsDict也是找到block存储term的,ordId/blocksize得知在哪个block
        field,            // 这里直接调用 addSortedNumberic, 可以直接写 doc 多值信息; 相比于 addSortedField, 没太多不同的
        new EmptyDocValuesProducer() {//[[ord2,ord1,ord3][ord6,ord1,ord2]],即存储doc里有几个ord,但是同时可以看到,内部数组的元素不是递增的
          @Override
          public SortedNumericDocValues getSortedNumeric(FieldInfo field) throws IOException {
            SortedSetDocValues values = valuesProducer.getSortedSet(field);
            return new SortedNumericDocValues() {

              long[] ords = LongsRef.EMPTY_LONGS;
              int i, docValueCount;

              @Override
              public long nextValue() throws IOException {
                return ords[i++];
              }

              @Override
              public int docValueCount() {
                return docValueCount;
              }

              @Override
              public boolean advanceExact(int target) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public int docID() {
                return values.docID();
              }

              @Override
              public int nextDoc() throws IOException {
                int doc = values.nextDoc();
                if (doc != NO_MORE_DOCS) {
                  docValueCount = values.docValueCount();
                  ords = ArrayUtil.grow(ords, docValueCount);
                  for (int j = 0; j < docValueCount; j++) {
                    ords[j] = values.nextOrd();
                  }
                  i = 0;
                }
                return doc;
              }

              @Override
              public int advance(int target) throws IOException {
                throw new UnsupportedOperationException();
              }

              @Override
              public long cost() {
                return values.cost();
              }
            };
          }
        },
        true);

    addTermsDict(valuesProducer.getSortedSet(field));
  }
}
