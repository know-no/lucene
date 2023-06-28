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
import java.util.Arrays;
import java.util.Map;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.NormsProducer;
import org.apache.lucene.codecs.TermVectorsWriter;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.util.Accountable;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.IntBlockPool;
import org.apache.lucene.util.RamUsageEstimator;

class TermVectorsConsumer extends TermsHash { // 调度词向量的构建的最上层逻辑，负责创建
  protected final Directory directory;        // 调度 TermVectorsConsumerPerField 以及 TermVectorWriter
  protected final SegmentInfo info;
  protected final Codec codec;
  TermVectorsWriter writer; // 词向量持久化

  /** Scratch term used by TermVectorsConsumerPerField.finishDocument. */
  final BytesRef flushTerm = new BytesRef();
  // 用来从TermVectorsConsumerPerField的bytePool中读取position信息
  /** Used by TermVectorsConsumerPerField when serializing the term vectors. */
  final ByteSliceReader vectorSliceReaderPos = new ByteSliceReader();

  final ByteSliceReader vectorSliceReaderOff = new ByteSliceReader();

  private boolean hasVectors;
  private int numVectorFields; // 和 perFields 参数不是同等大小的, doc级别的参数
  int lastDocID; //doc级别的变量,每一个开启词向量构建的Field，都有一个TermVectorsConsumerPerField，当一个doc处理完之后会把所有的
  private TermVectorsConsumerPerField[] perFields = new TermVectorsConsumerPerField[1];//perFields都序列化到90CompressingTermVectorsWriter中，然后重置为null
  // this accountable either holds the writer or one that returns null.
  // it's cleaner than checking if the writer is null all over the place
  Accountable accountable = Accountable.NULL_ACCOUNTABLE;

  TermVectorsConsumer(
      final IntBlockPool.Allocator intBlockAllocator,
      final ByteBlockPool.Allocator byteBlockAllocator,
      Directory directory,
      SegmentInfo info,
      Codec codec) {
    super(intBlockAllocator, byteBlockAllocator, Counter.newCounter(), null);
    this.directory = directory;
    this.info = info;
    this.codec = codec;
  }

  @Override
  void flush(
      Map<String, TermsHashPerField> fieldsToFlush,
      final SegmentWriteState state,
      Sorter.DocMap sortMap,
      NormsProducer norms)
      throws IOException {
    if (writer != null) {
      int numDocs = state.segmentInfo.maxDoc();
      assert numDocs > 0;
      // At least one doc in this run had term vectors enabled
      try { // // 把不存在词向量Filed的文档填充下
        fill(numDocs);
        assert state.segmentInfo != null;
        writer.finish(numDocs);// // 触发词向量索引文件持久化落盘
      } finally {
        IOUtils.close(writer);
      }
    }
  }

  /**
   * Fills in no-term-vectors for all docs we haven't seen since the last doc that had term vectors.
   */
  void fill(int docID) throws IOException {
    while (lastDocID < docID) {
      writer.startDocument(0);
      writer.finishDocument();
      lastDocID++;
    }
  }
  // 初始化一个 具体的writer, 如Lucene90CompressingTermVectorsWriter
  void initTermVectorsWriter() throws IOException {
    if (writer == null) {
      IOContext context = new IOContext(new FlushInfo(lastDocID, bytesUsed.get()));
      writer = codec.termVectorsFormat().vectorsWriter(directory, info, context);
      lastDocID = 0;
      accountable = writer;
    }
  }

  void setHasVectors() {
    hasVectors = true;
  }

  @Override
  void finishDocument(int docID) throws IOException { //

    if (!hasVectors) { // 没有词向量, 直接返回
      return;
    }
    // 按字段名排序TermVectorsConsumerPerField. 这里可以看出, perFields.length != numVectorFields, 后者是实际的元素个数
    // Fields in term vectors are UTF16 sorted:
    ArrayUtil.introSort(perFields, 0, numVectorFields);

    initTermVectorsWriter(); // 如果为空 ,就初始化 // 在finish之前, 都没有用到writer. writer的的start...finish在consumer finish之后

    fill(docID); // 不留空隙, lastDocIdWithVector ------ docID 之间的要填充满

    // Append term vectors to the real outputs:   //* writer开始处理doc, 把相关的词向量数据写到writer的缓存中
    writer.startDocument(numVectorFields);        //*
    for (int i = 0; i < numVectorFields; i++) {   //*
      perFields[i].finishDocument();              //*
    }                                             //*
    writer.finishDocument();                      //*

    assert lastDocID == docID : "lastDocID=" + lastDocID + " docID=" + docID;

    lastDocID++;

    super.reset();
    resetFields();
  }

  @Override
  public void abort() {
    try {
      super.abort();
    } finally {
      IOUtils.closeWhileHandlingException(writer);
      reset();
    }
  }

  void resetFields() { // 因为要开始一个新的doc的收集, 所以reset他们.
    Arrays.fill(perFields, null); // don't hang onto stuff from previous doc
    numVectorFields = 0;
  }

  @Override           // 负责调度,创建: TermVectorConsumerPerField
  public TermsHashPerField addField(FieldInvertState invertState, FieldInfo fieldInfo) { // 为每一个新增的field, 生成一个Consumer
    return new TermVectorsConsumerPerField(invertState, this, fieldInfo);
  }
  //当结束一个Field的所有term的处理之后，就把TermVectorsConsumerPerField存在perFields中,等待把数据都序列化到Lucene90CompressingTermVectorsWriter中
  void addFieldToFlush(TermVectorsConsumerPerField fieldToFlush) {
    if (numVectorFields == perFields.length) { // 会被 调度,创建出来的TermVectorsConsumerPerField调用, 以标识field 处理完毕
      int newSize = ArrayUtil.oversize(numVectorFields + 1, RamUsageEstimator.NUM_BYTES_OBJECT_REF);
      TermVectorsConsumerPerField[] newArray = new TermVectorsConsumerPerField[newSize];
      System.arraycopy(perFields, 0, newArray, 0, numVectorFields);
      perFields = newArray;
    }

    perFields[numVectorFields++] = fieldToFlush;
  }

  @Override
  void startDocument() { // 开始处理一个新的doc
    resetFields();
    numVectorFields = 0;
  }
}
