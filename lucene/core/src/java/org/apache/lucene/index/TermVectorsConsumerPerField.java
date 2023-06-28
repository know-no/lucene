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
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PayloadAttribute;
import org.apache.lucene.analysis.tokenattributes.TermFrequencyAttribute;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
// 和 FreqProxTermsWriterPerField 最大的区别是: TermVectorsCOnsumerPerField每处理完一个doc，就进行序列化然后重置等待处理下一个doc
final class TermVectorsConsumerPerField extends TermsHashPerField {
// 实际上在TermVectorsConsumerPerField中只负责调度Lucene90CompressingTermVectorsWriter进行操作
  private TermVectorsPostingsArray termVectorsPostingsArray;

  private final TermVectorsConsumer termsWriter; // 调度词向量的构建的最上层逻辑，负责创建
  private final FieldInvertState fieldState; // per-field信息, 与PerField类的FieldInvertState相绑定
  private final FieldInfo fieldInfo;

  private boolean doVectors;
  private boolean doVectorPositions;
  private boolean doVectorOffsets;
  private boolean doVectorPayloads;

  private OffsetAttribute offsetAttribute;
  private PayloadAttribute payloadAttribute;
  private TermFrequencyAttribute termFreqAtt;
  private final ByteBlockPool termBytePool;

  private boolean hasPayloads; // if enabled, and we actually saw any for this field

  TermVectorsConsumerPerField(
      FieldInvertState invertState, TermVectorsConsumer termsHash, FieldInfo fieldInfo) {
    super(
        2,
        termsHash.intPool,
        termsHash.bytePool,
        termsHash.termBytePool,
        termsHash.bytesUsed,
        null,
        fieldInfo.name,
        fieldInfo.getIndexOptions());
    this.termsWriter = termsHash;
    this.fieldInfo = fieldInfo;
    this.fieldState = invertState;
    termBytePool = termsHash.termBytePool;
  }

  /**
   * Called once per field per document if term vectors are enabled, to write the vectors to
   * RAMOutputStream, which is then quickly flushed to the real term vectors files in the Directory.
   */
  @Override
  void finish() {
    if (!doVectors || getNumTerms() == 0) {
      return;
    }                                 // TermVectors和FreqProx的差异就是, 前者每次处理完一个doc,就要finish
    termsWriter.addFieldToFlush(this);//对于某个doc, perfield已经处理完了自己field的信息. 可以被调用finishDocument了
  }

  void finishDocument() throws IOException {
    if (doVectors == false) { //
      return;
    }

    doVectors = false;

    final int numPostings = getNumTerms(); // 在term倒排中, 有多少个 term, 需要构造倒排链

    final BytesRef flushTerm = termsWriter.flushTerm;

    assert numPostings >= 0;

    // This is called once, after inverting all occurrences
    // of a given field in the doc.  At this point we flush
    // our hash into the DocWriter.

    TermVectorsPostingsArray postings = termVectorsPostingsArray;
    final TermVectorsWriter tv = termsWriter.writer; // Lucene90CompressingTermVectorsWriter

    sortTerms(); // 按照字典序排序
    final int[] termIDs = getSortedTermIDs(); // 下表是ord, 值是  termID
    // 开始用writer 处理一个field
    tv.startField(fieldInfo, numPostings, doVectorPositions, doVectorOffsets, hasPayloads);
    // 从bytepool中读取 pos, 读取offset
    final ByteSliceReader posReader = doVectorPositions ? termsWriter.vectorSliceReaderPos : null;
    final ByteSliceReader offReader = doVectorOffsets ? termsWriter.vectorSliceReaderOff : null;

    for (int j = 0; j < numPostings; j++) {
      final int termID = termIDs[j];
      final int freq = postings.freqs[termID];
      // 按照顺序,获取倒排的termID, 然后获取它的内容在bytepool里的地址, 通过地址, 获取内容
      // Get BytesRef
      termBytePool.setBytesRef(flushTerm, postings.textStarts[termID]);
      tv.startTerm(flushTerm, freq); // 准备序列化term的词向量信息

      if (doVectorPositions || doVectorOffsets) {
        if (posReader != null) {
          initReader(posReader, termID, 0); // 在bytepool里定位到stream0, termID的pos信息位置
        }
        if (offReader != null) {
          initReader(offReader, termID, 1);
        }
        tv.addProx(freq, posReader, offReader);
      }
      tv.finishTerm();
    }
    tv.finishField();

    reset();

    fieldInfo.setStoreTermVectors();
  }

  @Override // // 每次start都做这么多检查, 太麻烦了吧. 但是 start 不能缺少, 因为吧上一次处理此field的信息拿出来, 赋到当下在处理的state里
  boolean start(IndexableField field, boolean first) {
    super.start(field, first); // tv 需要 freq, 然后会判断是否需要offset,payload,需要的话,当前stream的attr提取器给到此类
    termFreqAtt = fieldState.termFreqAttribute; // todo: 但是为什么没有posIncre的?因为需要prox的时候,
    assert field.fieldType().indexOptions() != IndexOptions.NONE;

    if (first) { // 是否第一次处理, 不是话,保留此doc的此field的上一次状态,继续append

      if (getNumTerms() != 0) { // 清理上一次遗留的状态, 上一次doc收集的时候, 遇到了异常,且是可继续的异常
        // Only necessary if previous doc hit a
        // non-aborting exception while writing vectors in
        // this field:
        reset();
      }

      reinitHash(); // reinit 方法, 只有在reset方法里的 bytesHash.clear(false) 方法被调用之后, 才起作用. 否则, no effect

      hasPayloads = false;

      doVectors = field.fieldType().storeTermVectors();

      if (doVectors) {
        doVectorPositions = field.fieldType().storeTermVectorPositions();

        // Somewhat confusingly, unlike postings, you are
        // allowed to index TV offsets without TV positions:
        doVectorOffsets = field.fieldType().storeTermVectorOffsets();

        if (doVectorPositions) {
          doVectorPayloads = field.fieldType().storeTermVectorPayloads();
        } else {
          doVectorPayloads = false;
          if (field.fieldType().storeTermVectorPayloads()) {
            // TODO: move this check somewhere else, and impl the other missing ones
            throw new IllegalArgumentException(
                "cannot index term vector payloads without term vector positions (field=\""
                    + field.name()
                    + "\")");
          }
        }

      } else {
        if (field.fieldType().storeTermVectorOffsets()) {
          throw new IllegalArgumentException(
              "cannot index term vector offsets when term vectors are not indexed (field=\""
                  + field.name()
                  + "\")");
        }
        if (field.fieldType().storeTermVectorPositions()) {
          throw new IllegalArgumentException(
              "cannot index term vector positions when term vectors are not indexed (field=\""
                  + field.name()
                  + "\")");
        }
        if (field.fieldType().storeTermVectorPayloads()) {
          throw new IllegalArgumentException(
              "cannot index term vector payloads when term vectors are not indexed (field=\""
                  + field.name()
                  + "\")");
        }
      }
    } else {
      if (doVectors != field.fieldType().storeTermVectors()) {
        throw new IllegalArgumentException(
            "all instances of a given field name must have the same term vectors settings (storeTermVectors changed for field=\""
                + field.name()
                + "\")");
      }
      if (doVectorPositions != field.fieldType().storeTermVectorPositions()) {
        throw new IllegalArgumentException(
            "all instances of a given field name must have the same term vectors settings (storeTermVectorPositions changed for field=\""
                + field.name()
                + "\")");
      }
      if (doVectorOffsets != field.fieldType().storeTermVectorOffsets()) {
        throw new IllegalArgumentException(
            "all instances of a given field name must have the same term vectors settings (storeTermVectorOffsets changed for field=\""
                + field.name()
                + "\")");
      }
      if (doVectorPayloads != field.fieldType().storeTermVectorPayloads()) {
        throw new IllegalArgumentException(
            "all instances of a given field name must have the same term vectors settings (storeTermVectorPayloads changed for field=\""
                + field.name()
                + "\")");
      }
    }

    if (doVectors) {
      if (doVectorOffsets) {
        offsetAttribute = fieldState.offsetAttribute;
        assert offsetAttribute != null;
      }

      if (doVectorPayloads) {
        // Can be null:
        payloadAttribute = fieldState.payloadAttribute;
      } else {
        payloadAttribute = null;
      }
    }

    return doVectors;
  }

  void writeProx(TermVectorsPostingsArray postings, int termID) {
    if (doVectorOffsets) { // fieldState.offset不是之前更新过了吗??
      int startOffset = fieldState.offset + offsetAttribute.startOffset();//相加才能获得这个field实例(如field_a)的offset,连接之前的
      int endOffset = fieldState.offset + offsetAttribute.endOffset();
      // 向stream 1 里写入 offset, length
      writeVInt(1, startOffset - postings.lastOffsets[termID]);//上一个token的结束位置,和这一个的开始位置的length
      writeVInt(1, endOffset - startOffset); // token length
      postings.lastOffsets[termID] = endOffset;
    }                        // 1 写入offset, length

    if (doVectorPositions) { // 0 写入 pos, payload
      final BytesRef payload;
      if (payloadAttribute == null) {
        payload = null;
      } else {
        payload = payloadAttribute.getPayload();
      }
      // 差值存储, 所以 fieldState, 一定是当前的termId的positon了
      final int pos = fieldState.position - postings.lastPositions[termID];
      if (payload != null && payload.length > 0) {
        writeVInt(0, (pos << 1) | 1);
        writeVInt(0, payload.length);
        writeBytes(0, payload.bytes, payload.offset, payload.length);
        hasPayloads = true;
      } else {
        writeVInt(0, pos << 1);
      }
      postings.lastPositions[termID] = fieldState.position;
    }
  }

  @Override
  void newTerm(final int termID, final int docID) {//新建term(对于TermVectorsConsumerPerField是第一次见到,对于
    TermVectorsPostingsArray postings = termVectorsPostingsArray;// FreqProxTermsWriterPerField也是,
    // 因为调用了FreqProxTermsWriterPerField,才会调用到对于TermVectorsConsumerPerField的newTerm
    postings.freqs[termID] = getTermFreq(); // term在这里这个doc里出现了几次 //如果不是自定义的, 则是1. 不是自定义的, 则不能有pos信息
    postings.lastOffsets[termID] = 0; // 为啥是0?因为是 newTerm, 新建的倒排信息,之前都没出现过这个termId的信息
    postings.lastPositions[termID] = 0; // 所以没有last的信息, last的都是0

    writeProx(postings, termID);
  }

  @Override
  void addTerm(final int termID, final int docID) {
    TermVectorsPostingsArray postings = termVectorsPostingsArray;

    postings.freqs[termID] += getTermFreq(); // 更新下freq,// position, offset 反映在 attr 里, 不需要显式手动更新

    writeProx(postings, termID);
  }

  private int getTermFreq() {
    int freq = termFreqAtt.getTermFrequency();
    if (freq != 1) {
      if (doVectorPositions) {
        throw new IllegalArgumentException(
            "field \""
                + getFieldName()
                + "\": cannot index term vector positions while using custom TermFrequencyAttribute");
      }
      if (doVectorOffsets) {
        throw new IllegalArgumentException(
            "field \""
                + getFieldName()
                + "\": cannot index term vector offsets while using custom TermFrequencyAttribute");
      }
    }

    return freq;
  }

  @Override
  public void newPostingsArray() {
    termVectorsPostingsArray = (TermVectorsPostingsArray) postingsArray;
  }

  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    return new TermVectorsPostingsArray(size);
  }

  static final class TermVectorsPostingsArray extends ParallelPostingsArray {
    TermVectorsPostingsArray(int size) {
      super(size);
      freqs = new int[size];
      lastOffsets = new int[size];
      lastPositions = new int[size];
    }
    // 下标都是termID
    int[] freqs; // How many times this term occurred in the current doc//当前doc,当前field
    int[] lastOffsets; // Last offset we saw // delta压缩, startoffset, endoffset
    int[] lastPositions; // Last position where this term occurred // delata压缩

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new TermVectorsPostingsArray(size);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof TermVectorsPostingsArray;
      TermVectorsPostingsArray to = (TermVectorsPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(freqs, 0, to.freqs, 0, size);
      System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, size);
      System.arraycopy(lastPositions, 0, to.lastPositions, 0, size);
    }

    @Override
    int bytesPerPosting() {
      return super.bytesPerPosting() + 3 * Integer.BYTES;
    }
  }
}
