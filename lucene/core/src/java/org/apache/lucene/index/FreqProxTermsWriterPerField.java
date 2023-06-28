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
import org.apache.lucene.util.BytesRef;
// 存储了一个field的所有的 倒排信息, 在所有的文档处理完成之后, 才会刷写(进行序列化,持久化) // 这点和TermVectorsConsumerPerField不同
// TODO: break into separate freq and prox writers as
// codecs; make separate container (tii/tis/skip/*) that can
// be configured as any number of files 1..N
final class FreqProxTermsWriterPerField extends TermsHashPerField { // 疑问：这个倒排信息里怎么没有docID?
  // 有的，存在了FreqProxPostingsArray的lastDocCodes里? 且lastDocCodes会写入bytePool，而lastDocIds[]不会,见156，,169行
  private FreqProxPostingsArray freqProxPostingsArray;  // 后者是结束一个doc就会刷写,因为它是doc->field1,field2,... -> value...
  private final FieldInvertState fieldState;
  private final FieldInfo fieldInfo;

  final boolean hasFreq;
  final boolean hasProx;
  final boolean hasOffsets;
  PayloadAttribute payloadAttribute;
  OffsetAttribute offsetAttribute;
  TermFrequencyAttribute termFreqAtt;

  /** Set to true if any token had a payload in the current segment. */
  boolean sawPayloads;

  FreqProxTermsWriterPerField(
      FieldInvertState invertState,
      TermsHash termsHash,
      FieldInfo fieldInfo,
      TermsHashPerField nextPerField) {
    super(
        fieldInfo.getIndexOptions().compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0
            ? 2
            : 1,
        termsHash.intPool,
        termsHash.bytePool,
        termsHash.termBytePool,
        termsHash.bytesUsed,
        nextPerField,
        fieldInfo.name,
        fieldInfo.getIndexOptions());
    this.fieldState = invertState;
    this.fieldInfo = fieldInfo;
    hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    hasOffsets = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
  }

  @Override
  void finish() throws IOException {
    super.finish();
    if (sawPayloads) {
      fieldInfo.setStorePayloads();
    }
  }

  @Override
  boolean start(IndexableField f, boolean first) { // 考虑field_a, field_b, field_a
    super.start(f, first); // 把上次处理这个field的状态信息取出来,赋到当下
    termFreqAtt = fieldState.termFreqAttribute;     // 需要 freq
    payloadAttribute = fieldState.payloadAttribute; //     payload
    offsetAttribute = fieldState.offsetAttribute;   //     offset
    return true;
  }

  void writeProx(int termID, int proxCode) { // 以第一个vint的0号位byte判断是否有payloads
    if (payloadAttribute == null) {
      writeVInt(1, proxCode << 1);
    } else {
      BytesRef payload = payloadAttribute.getPayload();
      if (payload != null && payload.length > 0) {
        writeVInt(1, (proxCode << 1) | 1);
        writeVInt(1, payload.length);
        writeBytes(1, payload.bytes, payload.offset, payload.length);
        sawPayloads = true;
      } else {
        writeVInt(1, proxCode << 1);
      }
    }

    assert postingsArray == freqProxPostingsArray;
    freqProxPostingsArray.lastPositions[termID] = fieldState.position;
  }
  // 关于offset: https://juejin.cn/post/7139059809541685256#heading-21 说明: offsetAccum,是给一个doc,增加了许多同名field,要把他们连接起来的连接参数
  void writeOffsets(int termID, int offsetAccum) {
    final int startOffset = offsetAccum + offsetAttribute.startOffset();
    final int endOffset = offsetAccum + offsetAttribute.endOffset();
    assert startOffset - freqProxPostingsArray.lastOffsets[termID] >= 0;
    writeVInt(1, startOffset - freqProxPostingsArray.lastOffsets[termID]); // 写startOffset的差值
    writeVInt(1, endOffset - startOffset); // 再写下 end-start的值
    freqProxPostingsArray.lastOffsets[termID] = startOffset; // 记录下,记录在内存里
  }

  @Override
  void newTerm(final int termID, final int docID) { // 新建term的倒排表
    // First time we're seeing this term since the last
    // flush
    final FreqProxPostingsArray postings = freqProxPostingsArray;
    // new Term, 记录这个是为了帮助判断: 上一个doc的此term,是否处理完成，见addTerm:153行. 如果lastDocIDs[termID] != docID
    postings.lastDocIDs[termID] = docID; // 说明, 上一个doc的此Field里也有此term, 且我们是按照doc纬度处理的, 但docId不等,说明是下一个doc了,可以收尾上一个doc了
    if (!hasFreq) { // 无 频率信息 // 接上一行。另外，在newTerm，新建倒排表的情况后，如果没有addTerm调用（即说明此term值只在这个docID出现一次）
      assert postings.termFreqs == null;//所以，也不会将此docID信息写到bytePool，而只有在全局(所有doc)出现过一次以上才会写入bytepool（addTerm:156,169行）,只出现一次的就放在lastDocIds就好，用的时候从里面拿出来就好，见读取内存中的构建的倒排信息的类：FreqProxFields的367行
      postings.lastDocCodes[termID] = docID; // codes编码,是依据index option来的   // maxTermFreq = 1
      fieldState.maxTermFrequency = Math.max(1, fieldState.maxTermFrequency);//fieldState是每个doc的都不同的 reset
    } else { // 有频率信息
      postings.lastDocCodes[termID] = docID << 1; // 从来没遇到的term, hasFreq, 此刻freq==1,因为连term都是第一次见
      postings.termFreqs[termID] = getTermFreq(); // 但只是设置lastDocCodes,它本该代表 前一个 term 的情况,但没有前一个相同的term
      if (hasProx) {
        writeProx(termID, fieldState.position); // 向stream 1 写入 positon
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset); // 向 stream 1 写入 offset
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.maxTermFrequency = // 取最大的 term freq
          Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
    }
    fieldState.uniqueTermCount++;
  }

  @Override
  void addTerm(final int termID, final int docID) {
    final FreqProxPostingsArray postings = freqProxPostingsArray;
    assert !hasFreq || postings.termFreqs[termID] > 0;
    // 下文中: (docID != postings.lastDocIDs[termID]) 很重要. 这个对于统计 Freq很重要,因为freq必须在处理完doc后才完全知道
    if (!hasFreq) {
      assert postings.termFreqs == null;
      if (termFreqAtt.getTermFrequency() != 1) { // Analyzer 分析的时候添加了Term Freq,但是这个field的配置却是不需要
        throw new IllegalStateException(
            "field \""
                + getFieldName()
                + "\": must index term freq while using custom TermFrequencyAttribute"); //抱怨的是 !hasFreq
      }
      if (docID != postings.lastDocIDs[termID]) {//虽然是在处理docID的文档,但这也意味着 postings.lastDocIDs[termID]所代表的doc在termID下已经处理完成了
        // New document; now encode docCode for previous doc:
        assert docID > postings.lastDocIDs[termID]; //docID要递增
        writeVInt(0, postings.lastDocCodes[termID]);//向stream0 写入上一个doc编码(上一个doc在此termID下已经结束了), 现在才写,是因为它的term freq只有在doc处理结束才知晓
        postings.lastDocCodes[termID] = docID - postings.lastDocIDs[termID];//在 !hasFreq下, codes存储的是delta docID
        postings.lastDocIDs[termID] = docID; //处理新的文档,此刻lastDocCodes应该更新为此doc的编码, 且在不含有freq的分支下, 不用编码,直接delta上就好
        fieldState.uniqueTermCount++;
      }
    } else if (docID != postings.lastDocIDs[termID]) { // hasFreq and 处理新doc
      assert docID > postings.lastDocIDs[termID]
          : "id: " + docID + " postings ID: " + postings.lastDocIDs[termID] + " termID: " + termID;
      // Term not yet seen in the current doc but previously
      // seen in other doc(s) since the last flush

      // Now that we know doc freq for previous doc, // 现在已经全部处理完了上个doc, 所以知道上个doc的此term的freq了,所以可以写入code了
      // write it & lastDocCode     // freq分两种情况: 第一种: 1 第二种不是1.
      if (1 == postings.termFreqs[termID]) {
        writeVInt(0, postings.lastDocCodes[termID] | 1); // 是1 , 则合并写入
      } else {
        writeVInt(0, postings.lastDocCodes[termID]); // 不是1, 则分开写入
        writeVInt(0, postings.termFreqs[termID]); // 只有在 hasFreq, 且处理的是相同文档才会被update,见193行
      }

      // Init freq for the current document // 上一个doc的扫尾完成了, 现在初始化此doc的
      postings.termFreqs[termID] = getTermFreq();
      fieldState.maxTermFrequency =
          Math.max(postings.termFreqs[termID], fieldState.maxTermFrequency);
      postings.lastDocCodes[termID] = (docID - postings.lastDocIDs[termID]) << 1; // 还是delta压缩, 并且左移动一位
      postings.lastDocIDs[termID] = docID; //
      if (hasProx) {
        writeProx(termID, fieldState.position);
        if (hasOffsets) {
          postings.lastOffsets[termID] = 0;
          writeOffsets(termID, fieldState.offset);
        }
      } else {
        assert !hasOffsets;
      }
      fieldState.uniqueTermCount++;
    } else { // 还是处理当前的doc,但是已经有term了, 处理第一次add, 第二次add (add之前已经new过了,且还是同一个term)//docID == postings.lastDocIDs[termID]
      postings.termFreqs[termID] = Math.addExact(postings.termFreqs[termID], getTermFreq());
      fieldState.maxTermFrequency =
          Math.max(fieldState.maxTermFrequency, postings.termFreqs[termID]);
      if (hasProx) { // delta 压缩写position
        writeProx(termID, fieldState.position - postings.lastPositions[termID]);
        if (hasOffsets) {
          writeOffsets(termID, fieldState.offset);
        }
      }
    }
  }

  private int getTermFreq() { // 之所以只能返回1, 是因为此方法被调用的时候, field还没被处理完, freq只有在全部处理完token才是正确的
    int freq = termFreqAtt.getTermFrequency();     // 所以, 每次都是1, 但是会被记录下来
    if (freq != 1) {                               // 不过不是1, 则是用的自定义的attr, 不能有prox
      if (hasProx) {
        throw new IllegalStateException(
            "field \""
                + getFieldName()
                + "\": cannot index positions while using custom TermFrequencyAttribute");
      }
    }

    return freq;
  }

  @Override
  public void newPostingsArray() {
    freqProxPostingsArray = (FreqProxPostingsArray) postingsArray;
  }

  @Override
  ParallelPostingsArray createPostingsArray(int size) {
    boolean hasFreq = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    boolean hasProx = indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    boolean hasOffsets =
        indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS) >= 0;
    return new FreqProxPostingsArray(size, hasFreq, hasProx, hasOffsets);
  }

  static final class FreqProxPostingsArray extends ParallelPostingsArray {
    public FreqProxPostingsArray(
        int size, boolean writeFreqs, boolean writeProx, boolean writeOffsets) {
      super(size);
      if (writeFreqs) {
        termFreqs = new int[size];
      }
      lastDocIDs = new int[size];
      lastDocCodes = new int[size];
      if (writeProx) {
        lastPositions = new int[size];
        if (writeOffsets) {
          lastOffsets = new int[size];
        }
      } else {
        assert !writeOffsets;
      }
      // System.out.println("PA init freqs=" + writeFreqs + " pos=" + writeProx + " offs=" +
      // writeOffsets);
    }
    // 在内存中构建倒排表需要的字段, 每次更新term token都会改变
    int[] termFreqs; // # times this term occurs in the current doc
    int[] lastDocIDs; // Last docID where this term occurred
    int[] lastDocCodes; // Code for prior doc // 下标是termID,值是上一个出现这个term的文档id的编码：和lastDocIds的差别是,这个是处理写一个doc的时候才写入上一个的
    int[] lastPositions; // Last position where this term occurred      // lastDocCodes 并且每次在处理完一次add的时候,都会更新,但是在处理下个doc的是时候才会提交
    int[] lastOffsets; // Last endOffset where this term occurred //

    @Override
    ParallelPostingsArray newInstance(int size) {
      return new FreqProxPostingsArray(
          size, termFreqs != null, lastPositions != null, lastOffsets != null);
    }

    @Override
    void copyTo(ParallelPostingsArray toArray, int numToCopy) {
      assert toArray instanceof FreqProxPostingsArray;
      FreqProxPostingsArray to = (FreqProxPostingsArray) toArray;

      super.copyTo(toArray, numToCopy);

      System.arraycopy(lastDocIDs, 0, to.lastDocIDs, 0, numToCopy);
      System.arraycopy(lastDocCodes, 0, to.lastDocCodes, 0, numToCopy);
      if (lastPositions != null) {
        assert to.lastPositions != null;
        System.arraycopy(lastPositions, 0, to.lastPositions, 0, numToCopy);
      }
      if (lastOffsets != null) {
        assert to.lastOffsets != null;
        System.arraycopy(lastOffsets, 0, to.lastOffsets, 0, numToCopy);
      }
      if (termFreqs != null) {
        assert to.termFreqs != null;
        System.arraycopy(termFreqs, 0, to.termFreqs, 0, numToCopy);
      }
    }

    @Override
    int bytesPerPosting() {
      int bytes = ParallelPostingsArray.BYTES_PER_POSTING + 2 * Integer.BYTES;
      if (lastPositions != null) {
        bytes += Integer.BYTES;
      }
      if (lastOffsets != null) {
        bytes += Integer.BYTES;
      }
      if (termFreqs != null) {
        bytes += Integer.BYTES;
      }

      return bytes;
    }
  }
}
