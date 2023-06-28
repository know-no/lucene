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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.DirectBytesStartArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.packed.PackedInts;
import org.apache.lucene.util.packed.PackedLongValues;

/**
 * Buffers up pending byte[] per doc, deref and sorting via int ord, then flushes when segment
 * flushes.
 */     // todo 疑问: 怎么保存了 docId -> docValue的关系的呢. (以为TermsDict是按照字典序的排序的)
class SortedDocValuesWriter extends DocValuesWriter<SortedDocValues> {
  final BytesRefHash hash;
  private final PackedLongValues.Builder pending;
  private final DocsWithFieldSet docsWithField;//是用的fixbitset数据结构. 一方面, docId的写入是按照顺序写入的. 另外一个,内部是bitMap,所以可以很方便的
  private final Counter iwBytesUsed;          // 按照从小到大,或者从大到小的顺序返回拥有的docId
  private long bytesUsed; // this currently only tracks differences in 'pending'
  private final FieldInfo fieldInfo;
  private int lastDocID = -1;

  private PackedLongValues finalOrds;
  private int[] finalSortedValues;
  private int[] finalOrdMap;

  public SortedDocValuesWriter(FieldInfo fieldInfo, Counter iwBytesUsed, ByteBlockPool pool) {
    this.fieldInfo = fieldInfo;
    this.iwBytesUsed = iwBytesUsed;
    hash =
        new BytesRefHash(
            pool,
            BytesRefHash.DEFAULT_CAPACITY,
            new DirectBytesStartArray(BytesRefHash.DEFAULT_CAPACITY, iwBytesUsed));
    pending = PackedLongValues.deltaPackedBuilder(PackedInts.COMPACT);
    docsWithField = new DocsWithFieldSet();
    bytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(bytesUsed);
  }

  public void addValue(int docID, BytesRef value) {
    if (docID <= lastDocID) {
      throw new IllegalArgumentException(
          "DocValuesField \""
              + fieldInfo.name
              + "\" appears more than once in this document (only one value is allowed per field)");
    }
    if (value == null) {
      throw new IllegalArgumentException(
          "field \"" + fieldInfo.name + "\": null value not allowed");
    }
    if (value.length > (BYTE_BLOCK_SIZE - 2)) {
      throw new IllegalArgumentException(
          "DocValuesField \""
              + fieldInfo.name
              + "\" is too large, must be <= "
              + (BYTE_BLOCK_SIZE - 2));
    }

    addOneValue(value); // 写入writer
    docsWithField.add(docID);

    lastDocID = docID;
  }

  private void addOneValue(BytesRef value) {
    int termID = hash.add(value); // 获得termID // hash并不是全局index的, 返回的termID,只是这个段内的
    if (termID < 0) { // <0 说明不是第一次见
      termID = -termID - 1;
    } else {
      // reserve additional space for each unique value:
      // 1. when indexing, when hash is 50% full, rehash() suddenly needs 2*size ints.
      //    TODO: can this same OOM happen in THPF?
      // 2. when flushing, we need 1 int per value (slot in the ordMap).
      iwBytesUsed.addAndGet(2 * Integer.BYTES);
    }

    pending.add(termID); // 临时放进 内存压缩的存储; 收集 docvalue 的termid
    updateBytesUsed();
  }

  private void updateBytesUsed() {
    final long newBytesUsed = pending.ramBytesUsed() + docsWithField.ramBytesUsed();
    iwBytesUsed.addAndGet(newBytesUsed - bytesUsed);
    bytesUsed = newBytesUsed;
  }

  @Override
  SortedDocValues getDocValues() {
    int valueCount = hash.size(); // 有多少个 term
    if (finalSortedValues == null) {
      updateBytesUsed();
      assert finalOrdMap == null && finalOrds == null;
      finalSortedValues = hash.sort(); // 根据背后的binary来排序, 返回的termID数组, 是按照对应的biunary排序的,段内排序,不是index全局
      finalOrds = pending.build();//pending一直在收集termID,这里可以理解为转为读模型//可以理解为docId2TermId数组,下标是段内docId,只是TermId
      finalOrdMap = new int[valueCount];//更准确的说是 docId2TermIdLinkedMap,因为数组的下标是连续的,012345,但可能2没有field docvalue,所以LinedMap更合理,且map的key掌握在docsWithField手里
    }
    for (int ord = 0; ord < valueCount; ord++) {
      finalOrdMap[finalSortedValues[ord]] = ord; // finalSortedValues[ord], if ord==0 第一个ord的term的termID,
    }                                            // 得到 finalOrdMap:  termID -> ord
    return new BufferedSortedDocValues( // finalOrds 只是为了得到doc的docvalue的ord
        hash, finalOrds, finalSortedValues, finalOrdMap, docsWithField.iterator()); //
  }

  private int[] sortDocValues(int maxDoc, Sorter.DocMap sortMap, SortedDocValues oldValues)
      throws IOException {
    int[] ords = new int[maxDoc];
    Arrays.fill(ords, -1);
    int docID;
    while ((docID = oldValues.nextDoc()) != NO_MORE_DOCS) {
      int newDocID = sortMap.oldToNew(docID);
      ords[newDocID] = oldValues.ordValue();
    }
    return ords;
  }

  @Override
  public void flush(SegmentWriteState state, Sorter.DocMap sortMap, DocValuesConsumer dvConsumer)
      throws IOException {
    final int valueCount = hash.size();
    if (finalOrds == null) {
      updateBytesUsed();
      finalSortedValues = hash.sort();
      finalOrds = pending.build();
      finalOrdMap = new int[valueCount];
      for (int ord = 0; ord < valueCount; ord++) {
        finalOrdMap[finalSortedValues[ord]] = ord;
      }
    }

    final int[] sorted;
    if (sortMap != null) {
      sorted =
          sortDocValues(
              state.segmentInfo.maxDoc(),
              sortMap,
              new BufferedSortedDocValues(
                  hash, finalOrds, finalSortedValues, finalOrdMap, docsWithField.iterator()));
    } else {
      sorted = null;
    }
    dvConsumer.addSortedField(
        fieldInfo,
        new EmptyDocValuesProducer() {
          @Override
          public SortedDocValues getSorted(FieldInfo fieldInfoIn) {
            if (fieldInfoIn != fieldInfo) {
              throw new IllegalArgumentException("wrong fieldInfo");
            }
            final SortedDocValues buf =
                new BufferedSortedDocValues(
                    hash, finalOrds, finalSortedValues, finalOrdMap, docsWithField.iterator());
            if (sorted == null) {
              return buf;
            }
            return new SortingSortedDocValues(buf, sorted);
          }
        });
  }

  static class BufferedSortedDocValues extends SortedDocValues {
    final BytesRefHash hash;
    final BytesRef scratch = new BytesRef();
    final int[] sortedValues; // 下标是ord, 只是某个termID, 即 ord -> termID 的映射 ; 排序不是按照termID大小拍的, 而是term的字典序
    final int[] ordMap;//termID -> ord的映射;给定一个termID,得到ord顺序,就可以在TermDict中找到具体的term. 如idx=0,value=5即term0在dict中排名第五
    private int ord;        // TermDict中的term是按照 term 排序的, 给定一个id,就可以找到ta. TermDict存储该字段下所有的term，因为这里的term都是顺序的，所以采取前缀压缩。
    final PackedLongValues.Iterator iter;
    final DocIdSetIterator docsWithField;

    public BufferedSortedDocValues(
        BytesRefHash hash,
        PackedLongValues docToOrd, // pending.add(termID),所以每次get都是按照doc顺序get的termID
        int[] sortedValues,
        int[] ordMap,
        DocIdSetIterator docsWithField) {
      this.hash = hash;
      this.sortedValues = sortedValues;
      this.iter = docToOrd.iterator();
      this.ordMap = ordMap;
      this.docsWithField = docsWithField;
    }

    @Override
    public int docID() {
      return docsWithField.docID();
    }

    @Override
    public int nextDoc() throws IOException {
      int docID = docsWithField.nextDoc(); // 本质就是doc插入的顺序
      if (docID != NO_MORE_DOCS) {
        ord = Math.toIntExact(iter.next()); // 获得term id, 然后复制给ord, 其实应该用一个临时变量 termId的
        ord = ordMap[ord]; // termId -> ord // 以及termid,获取到ord
      }
      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      throw new UnsupportedOperationException();
    }

    @Override
    public long cost() {
      return docsWithField.cost();
    }

    @Override
    public int ordValue() {
      return ord;
    }

    @Override
    public BytesRef lookupOrd(int ord) {
      assert ord >= 0 && ord < sortedValues.length;
      assert sortedValues[ord] >= 0 && sortedValues[ord] < sortedValues.length;
      hash.get(sortedValues[ord], scratch);// sortedValues: ordId -> termId
      return scratch;
    }

    @Override
    public int getValueCount() {
      return hash.size();
    }
  }

  static class SortingSortedDocValues extends SortedDocValues {

    private final SortedDocValues in;
    private final int[] ords;
    private int docID = -1;

    SortingSortedDocValues(SortedDocValues in, int[] ords) {
      this.in = in;
      this.ords = ords;
      assert ords != null;
    }

    @Override
    public int docID() {
      return docID;
    }

    @Override
    public int nextDoc() {
      while (true) {
        docID++;
        if (docID == ords.length) {
          docID = NO_MORE_DOCS;
          break;
        }
        if (ords[docID] != -1) {
          break;
        }
        // skip missing docs
      }

      return docID;
    }

    @Override
    public int advance(int target) {
      throw new UnsupportedOperationException("use nextDoc instead");
    }

    @Override
    public boolean advanceExact(int target) throws IOException {
      // needed in IndexSorter#StringSorter
      docID = target;
      return ords[target] != -1;
    }

    @Override
    public int ordValue() {
      return ords[docID];
    }

    @Override
    public long cost() {
      return in.cost();
    }

    @Override
    public BytesRef lookupOrd(int ord) throws IOException {
      return in.lookupOrd(ord);
    }

    @Override
    public int getValueCount() {
      return in.getValueCount();
    }
  }
}
