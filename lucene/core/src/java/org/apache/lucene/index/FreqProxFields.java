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
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.FreqProxTermsWriterPerField.FreqProxPostingsArray;
import org.apache.lucene.util.AttributeSource;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;

/**
 * Implements limited (iterators only, no stats) {@link Fields} interface over the in-RAM buffered
 * fields/terms/postings, to flush postings through the PostingsFormat.
 */
class FreqProxFields extends Fields { // 迭代field所有的term，对每一个term用FreqProxTermsEnum封装，FreqProxTermsEnum根据是否只需要stream0的数据还是都需要分别创建FreqProxDocsEnum和FreqProxPostingsEnum最终完成倒排数据的读取。
  final Map<String, FreqProxTermsWriterPerField> fields = new LinkedHashMap<>();

  public FreqProxFields(List<FreqProxTermsWriterPerField> fieldList) {
    // NOTE: fields are already sorted by field name
    for (FreqProxTermsWriterPerField field : fieldList) {
      fields.put(field.getFieldName(), field);
    }
  }

  @Override
  public Iterator<String> iterator() {
    return fields.keySet().iterator();
  }

  @Override
  public Terms terms(String field) throws IOException { // name 找 Terms
    FreqProxTermsWriterPerField perField = fields.get(field);
    return perField == null ? null : new FreqProxTerms(perField);// 字段中所有term的倒排信息的获取逻辑都在 FreqProxTerms 中
  }

  @Override
  public int size() {
    throw new UnsupportedOperationException();
  }

  private static class FreqProxTerms extends Terms {// 封装了单个Field的信息
    final FreqProxTermsWriterPerField terms;//包含了我们在内存中倒排的所有信息

    public FreqProxTerms(FreqProxTermsWriterPerField terms) {
      this.terms = terms;
    }

    @Override
    public TermsEnum iterator() {//返回term的迭代器
      FreqProxTermsEnum termsEnum = new FreqProxTermsEnum(terms);
      termsEnum.reset();
      return termsEnum;
    }

    @Override
    public long size() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumTotalTermFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public long getSumDocFreq() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int getDocCount() {
      throw new UnsupportedOperationException();
    }

    @Override // 判断
    public boolean hasFreqs() {
      return terms.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS) >= 0;
    }

    @Override
    public boolean hasOffsets() {
      // NOTE: the in-memory buffer may have indexed offsets
      // because that's what FieldInfo said when we started,
      // but during indexing this may have been downgraded:
      return terms.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS_AND_OFFSETS)
          >= 0;
    }

    @Override
    public boolean hasPositions() {
      // NOTE: the in-memory buffer may have indexed positions
      // because that's what FieldInfo said when we started,
      // but during indexing this may have been downgraded:
      return terms.indexOptions.compareTo(IndexOptions.DOCS_AND_FREQS_AND_POSITIONS) >= 0;
    }

    @Override
    public boolean hasPayloads() {
      return terms.sawPayloads;
    }
  }
  // 1. 迭代获取所有的term  2. 使用postings 方法获取当时所在term的倒排数据链
  private static class FreqProxTermsEnum extends BaseTermsEnum {
    final FreqProxTermsWriterPerField terms;
    final int[] sortedTermIDs;//获取排序过后的termId,下标是ord,值是term的id//
    final FreqProxPostingsArray postingsArray;//之前遗留的问题： 最后处理的term的文档的id和频率没有写入bytePool，它存储在postingsArray中
    final BytesRef scratch = new BytesRef();//当前处理的term
    final int numTerms;
    int ord; // ord, 递增的数字，用ord， 从sortedTermIDs里获取对应的term

    FreqProxTermsEnum(FreqProxTermsWriterPerField terms) {
      this.terms = terms;
      this.numTerms = terms.getNumTerms();//获取总共有多少个独立的term
      sortedTermIDs = terms.getSortedTermIDs();//获取排序过后的termId,下标是ord,值是term的id//
      assert sortedTermIDs != null;
      postingsArray = (FreqProxPostingsArray) terms.postingsArray;
    }

    public void reset() {
      ord = -1;
    }

    @Override
    public SeekStatus seekCeil(BytesRef text) {
      // TODO: we could instead keep the BytesRefHash
      // intact so this is a hash lookup

      // binary search: // 为啥可以用二分法搜索， 因为term的值内容在写入bytepool之前，经过了sort,所以它是有序的
      int lo = 0;
      int hi = numTerms - 1;
      while (hi >= lo) {
        int mid = (lo + hi) >>> 1;
        int textStart = postingsArray.textStarts[sortedTermIDs[mid]]; // 获取次序mid对应的term的值内容本身在bytepool的位置
        terms.bytePool.setBytesRef(scratch, textStart); // 可以从里面读取，并且放到 scratch里面
        int cmp = scratch.compareTo(text); // 和我们要搜的那个对比，
        if (cmp < 0) {
          lo = mid + 1;
        } else if (cmp > 0) {
          hi = mid - 1;
        } else {
          // found:
          ord = mid;
          assert term().compareTo(text) == 0;
          return SeekStatus.FOUND; // 找到的时候，就直接返回了
        }
      }

      // not found:
      ord = lo; // 放到最小的lo上
      if (ord >= numTerms) { // 可能发生吗？ 可能的， 比如 numTerms = 1， 则lo和hi都等于0，且搜的term字典序大于这个唯一的term
        return SeekStatus.END; // 则lo， 会等于1.
      } else { // 还是要把当前的第一个小于所查的那个赋值来
        int textStart = postingsArray.textStarts[sortedTermIDs[ord]];
        terms.bytePool.setBytesRef(scratch, textStart);
        assert term().compareTo(text) > 0;
        return SeekStatus.NOT_FOUND;
      }
    }

    @Override
    public void seekExact(long ord) { // 这个ord如果超了就 直接runtime异常了呀
      this.ord = (int) ord;
      int textStart = postingsArray.textStarts[sortedTermIDs[this.ord]];
      terms.bytePool.setBytesRef(scratch, textStart);
    }

    @Override
    public BytesRef next() {
      ord++;
      if (ord >= numTerms) {
        return null;
      } else {
        int textStart = postingsArray.textStarts[sortedTermIDs[ord]];
        terms.bytePool.setBytesRef(scratch, textStart);
        return scratch;
      }
    }

    @Override
    public BytesRef term() {
      return scratch;
    }

    @Override
    public long ord() {
      return ord;
    }

    @Override
    public int docFreq() { // per-term不存储doc freq
      // We do not store this per-term, and we cannot
      // implement this at merge time w/o an added pass
      // through the postings:
      throw new UnsupportedOperationException();
    }

    @Override
    public long totalTermFreq() {
      // We do not store this per-term, and we cannot
      // implement this at merge time w/o an added pass
      // through the postings:
      throw new UnsupportedOperationException();
    }
    //获取倒排信息的迭代器。如果需要需要stream0中的倒排信息则使用FreqProxDocsEnum，否则使用FreqProxPostingsEnum。
    @Override // 根据传递的flags判断是否需要 position 信息， 不同情况下用不同的  PostingsEnum 子类
    public PostingsEnum postings(PostingsEnum reuse, int flags) {
      if (PostingsEnum.featureRequested(flags, PostingsEnum.POSITIONS)) {
        FreqProxPostingsEnum posEnum;

        if (!terms.hasProx) { // 算是一个防御式编程的方式吧
          // Caller wants positions but we didn't index them;
          // don't lie:
          throw new IllegalArgumentException("did not index positions");
        }

        if (!terms.hasOffsets && PostingsEnum.featureRequested(flags, PostingsEnum.OFFSETS)) {
          // Caller wants offsets but we didn't index them;
          // don't lie:
          throw new IllegalArgumentException("did not index offsets");
        }

        if (reuse instanceof FreqProxPostingsEnum) { // 已经有了这个值， 只是重复读一下
          posEnum = (FreqProxPostingsEnum) reuse;
          if (posEnum.postingsArray != postingsArray) {//进一步判断postingsArray来确定是有复用reuse
            posEnum = new FreqProxPostingsEnum(terms, postingsArray);
          }
        } else {
          posEnum = new FreqProxPostingsEnum(terms, postingsArray);
        }
        posEnum.reset(sortedTermIDs[ord]); // 好奇， 为什么不做成 PostingsEnum 的父类方法
        return posEnum;
      }

      FreqProxDocsEnum docsEnum;
      // 判断, 需要freq， 但是不含有freq， 直接标错
      if (!terms.hasFreq && PostingsEnum.featureRequested(flags, PostingsEnum.FREQS)) {
        // Caller wants freqs but we didn't index them;
        // don't lie:
        throw new IllegalArgumentException("did not index freq");
      }
      // 否则， 只有 docId 和 freq
      if (reuse instanceof FreqProxDocsEnum) {
        docsEnum = (FreqProxDocsEnum) reuse;
        if (docsEnum.postingsArray != postingsArray) {
          docsEnum = new FreqProxDocsEnum(terms, postingsArray);
        }
      } else {
        docsEnum = new FreqProxDocsEnum(terms, postingsArray);
      }
      docsEnum.reset(sortedTermIDs[ord]);
      return docsEnum;
    }

    @Override
    public ImpactsEnum impacts(int flags) throws IOException {
      throw new UnsupportedOperationException();
    }

    /**
     * Expert: Returns the TermsEnums internal state to position the TermsEnum without re-seeking
     * the term dictionary.
     *
     * <p>NOTE: A seek by {@link TermState} might not capture the {@link AttributeSource}'s state.
     * Callers must maintain the {@link AttributeSource} states separately
     *
     * @see TermState
     * @see #seekExact(BytesRef, TermState)
     */
    @Override
    public TermState termState() throws IOException {
      return new TermState() {
        @Override
        public void copyFrom(TermState other) {
          throw new UnsupportedOperationException();
        }
      };
    }
  }
// 只需要term所在文档id及其在文档中的出现频率使用FreqProxDocsEnum。
  private static class FreqProxDocsEnum extends PostingsEnum {

    final FreqProxTermsWriterPerField terms;
    final FreqProxPostingsArray postingsArray; // 这个值， 也是上层从terms拿出来，然后赋值的
    final ByteSliceReader reader = new ByteSliceReader(); // SliceReader从bytepool的一条stream里面读取的
    final boolean readTermFreq;
    int docID = -1;
    int freq;
    boolean ended;
    int termID;

    public FreqProxDocsEnum(
        FreqProxTermsWriterPerField terms, FreqProxPostingsArray postingsArray) {
      this.terms = terms;
      this.postingsArray = postingsArray;
      this.readTermFreq = terms.hasFreq;
    }

    public void reset(int termID) {
      this.termID = termID; // 定位到这个termID
      terms.initReader(reader, termID, 0);
      ended = false;
      docID = -1;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      // Don't lie here ... don't want codecs writings lots
      // of wasted 1s into the index:
      if (!readTermFreq) {
        throw new IllegalStateException("freq was not indexed");
      } else {
        return freq;
      }
    }

    @Override
    public int nextPosition() throws IOException {
      return -1;
    }

    @Override
    public int startOffset() throws IOException {
      return -1;
    }

    @Override
    public int endOffset() throws IOException {
      return -1;
    }

    @Override
    public BytesRef getPayload() throws IOException {
      return null;
    }

    @Override
    public int nextDoc() throws IOException {
      if (docID == -1) {
        docID = 0;//哈哈哈哈哈，不论如何，先设置为0，是为了方便377行的 docID += code或379行的 += 逻辑，差值存储，第一个是原值，所以得加0，不能-1
      }
      if (reader.eof()) {
        if (ended) {
          return NO_MORE_DOCS;
        } else { // 虽然读到末尾没数据可以读了; 见FreqProxTermsWriterPerField的addTerm：120行
          ended = true; // 并且，写入的时候，写入的是lastDocIDs[]，和termFreqs[], 还是原值
          docID = postingsArray.lastDocIDs[termID];
          if (readTermFreq) {
            freq = postingsArray.termFreqs[termID];
          }
        }
      } else {
        int code = reader.readVInt();
        if (!readTermFreq) {
          docID += code;
        } else {
          docID += code >>> 1; // 和写入的时候呼应， (docID | 1/0)判断是否有freq信息
          if ((code & 1) != 0) {
            freq = 1;
          } else {
            freq = reader.readVInt();
          }
        }

        assert docID != postingsArray.lastDocIDs[termID];
      }

      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }
  }

  private static class FreqProxPostingsEnum extends PostingsEnum {

    final FreqProxTermsWriterPerField terms;
    final FreqProxPostingsArray postingsArray;
    final ByteSliceReader reader = new ByteSliceReader();
    final ByteSliceReader posReader = new ByteSliceReader();
    final boolean readOffsets;
    int docID = -1;
    int freq;
    int pos;
    int startOffset;
    int endOffset;
    int posLeft;
    int termID;
    boolean ended;
    boolean hasPayload;
    BytesRefBuilder payload = new BytesRefBuilder();

    public FreqProxPostingsEnum( //它和FreqProxDocsEnum的差别是，后者只需要term所在文档id及其在文档中的出现频率，而它还有position信息,以及可能的payloads
        FreqProxTermsWriterPerField terms, FreqProxPostingsArray postingsArray) {
      this.terms = terms;
      this.postingsArray = postingsArray;
      this.readOffsets = terms.hasOffsets;
      assert terms.hasProx;
      assert terms.hasFreq;
    }

    public void reset(int termID) {
      this.termID = termID;
      terms.initReader(reader, termID, 0);
      terms.initReader(posReader, termID, 1);
      ended = false;
      docID = -1;
      posLeft = 0;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int freq() {
      return freq;
    }

    @Override
    public int nextDoc() throws IOException {
      if (docID == -1) {
        docID = 0;
      }
      while (posLeft != 0) { // 如果当前docID的stream1的信息还没处理完，则通过  nextPosition 都忽略掉
        nextPosition();
      }

      if (reader.eof()) {
        if (ended) {
          return NO_MORE_DOCS;
        } else {
          ended = true; // 我直接来个 同理可得
          docID = postingsArray.lastDocIDs[termID];
          freq = postingsArray.termFreqs[termID];
        }
      } else {
        int code = reader.readVInt();
        docID += code >>> 1;
        if ((code & 1) != 0) {
          freq = 1;
        } else {
          freq = reader.readVInt();
        }

        assert docID != postingsArray.lastDocIDs[termID];
      }

      posLeft = freq;
      pos = 0;
      startOffset = 0;
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      throw new UnsupportedOperationException();
    }

    @Override
    public int nextPosition() throws IOException {
      assert posLeft > 0;
      posLeft--;
      int code = posReader.readVInt();
      pos += code >>> 1;
      if ((code & 1) != 0) {
        hasPayload = true;
        // has a payload
        payload.setLength(posReader.readVInt());
        payload.grow(payload.length());
        posReader.readBytes(payload.bytes(), 0, payload.length());
      } else {
        hasPayload = false;
      }

      if (readOffsets) {
        startOffset += posReader.readVInt();
        endOffset = startOffset + posReader.readVInt();
      }

      return pos;
    }

    @Override
    public int startOffset() {
      if (!readOffsets) {
        throw new IllegalStateException("offsets were not indexed");
      }
      return startOffset;
    }

    @Override
    public int endOffset() {
      if (!readOffsets) {
        throw new IllegalStateException("offsets were not indexed");
      }
      return endOffset;
    }

    @Override
    public BytesRef getPayload() {
      if (hasPayload) {
        return payload.get();
      } else {
        return null;
      }
    }
  }
}
