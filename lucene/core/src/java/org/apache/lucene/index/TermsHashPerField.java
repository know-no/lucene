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
import org.apache.lucene.util.ByteBlockPool;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefHash;
import org.apache.lucene.util.BytesRefHash.BytesStartArray;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IntBlockPool;

/**
 * This class stores streams of information per term without knowing the size of the stream ahead of
 * time. Each stream typically encodes one level of information like term frequency per document or
 * term proximity. Internally this class allocates a linked list of slices that can be read by a
 * {@link ByteSliceReader} for each term. Terms are first deduplicated in a {@link BytesRefHash}
 * once this is done internal data-structures point to the current offset of each stream that can be
 * written to.
 */ // 存储每个term的美中信息,并且提前不知道大小. 每个stream是一种层级的信息,想 term-freq-per-doc, 或者 term proximity
abstract class TermsHashPerField implements Comparable<TermsHashPerField> {//倒排表的核心类.streamCount是作为参数传入的,决定它大小的是indexOptions
  private static final int HASH_INIT_SIZE = 4;

  private final TermsHashPerField nextPerField; // 倒排和词向量都会处理term信息，所以倒排和词向量是使用责任链的模式实现，nextPerField就是下一个要处理term信息的组件
  private final IntBlockPool intPool; // 记录某个term下次应该从byteblockpool的那个位置写
  final ByteBlockPool bytePool;//对于每个term,存一个int指向bytePool // 记录term和term的相关的倒排信息，倒排信息是以stream的方式写入的
  // for each term we store an integer per stream that points into the bytePool above
  // the address is updated once data is written to the stream to point to the next free offset
  // in the terms stream. The start address for the stream is stored in
  // postingsArray.byteStarts[termId]
  // This is initialized in the #addTerm method, either to a brand new per term stream if the term
  // is new or
  // to the addresses where the term stream was written to when we saw it the last time.
  private int[] termStreamAddressBuffer;
  private int streamAddressOffset; // 这offset in termStreamAddressBuffer，
  private final int streamCount;
  private final String fieldName;
  final IndexOptions indexOptions;
  /* This stores the actual term bytes for postings and offsets into the parent hash in the case that this
   * TermsHashPerField is hashing term vectors.*/
  private final BytesRefHash bytesHash; // 存储着 term 的token 的bytes. 并且是一个自实现的map. 对于FreqProxTermsWriterPerField而言
                              //提供 token.bytes -> termId ; 对于 TermVectorsConsumcerPerField而言
  ParallelPostingsArray postingsArray; // 倒排链数组.  postingsArray --termID-> intPool的下标x --intPool-> 在bytePool中的地址
  private int lastDocID; // only with assert

  /**
   * streamCount: how many streams this field stores per term. E.g. doc(+freq) is 1 stream,
   * prox+offset is a second.
   */
  TermsHashPerField(
      int streamCount,
      IntBlockPool intPool,
      ByteBlockPool bytePool,
      ByteBlockPool termBytePool,
      Counter bytesUsed,
      TermsHashPerField nextPerField,
      String fieldName,
      IndexOptions indexOptions) {
    this.intPool = intPool;
    this.bytePool = bytePool;
    this.streamCount = streamCount;
    this.fieldName = fieldName;
    this.nextPerField = nextPerField;
    assert indexOptions != IndexOptions.NONE;
    this.indexOptions = indexOptions;
    PostingsBytesStartArray byteStarts = new PostingsBytesStartArray(this, bytesUsed);
    bytesHash = new BytesRefHash(termBytePool, HASH_INIT_SIZE, byteStarts);
  }

  void reset() {
    bytesHash.clear(false);
    sortedTermIDs = null;
    if (nextPerField != null) {
      nextPerField.reset();
    }
  }

  final void initReader(ByteSliceReader reader, int termID, int stream) {
    assert stream < streamCount;
    int streamStartOffset = postingsArray.addressOffset[termID]; // 两跳
    final int[] streamAddressBuffer =
        intPool.buffers[streamStartOffset >> IntBlockPool.INT_BLOCK_SHIFT];
    final int offsetInAddressBuffer = streamStartOffset & IntBlockPool.INT_BLOCK_MASK;
    reader.init(
        bytePool, // 为什么要加上stream*FIRST_LEVEL_SIZE? 因为如果是stream1的话，它的slice是稍后开始的，
        postingsArray.byteStarts[termID] + stream * ByteBlockPool.FIRST_LEVEL_SIZE, // byteStarts[termID]+1*ByteBlockPool.FIRST_LEVEL_SIZE
        streamAddressBuffer[offsetInAddressBuffer + stream]); //第三个参数是结束位置   //接上一行： 存的是stream0 的第一个slice
  }

  private int[] sortedTermIDs;

  /**
   * Collapse the hash table and sort in-place; also sets this.sortedTermIDs to the results This
   * method must not be called twice unless {@link #reset()} or {@link #reinitHash()} was called.
   */
  final void sortTerms() {
    assert sortedTermIDs == null;
    sortedTermIDs = bytesHash.sort();
  }

  /** Returns the sorted term IDs. {@link #sortTerms()} must be called before */
  final int[] getSortedTermIDs() {
    assert sortedTermIDs != null;
    return sortedTermIDs;
  }

  final void reinitHash() {
    sortedTermIDs = null;
    bytesHash.reinit();
  }

  private boolean doNextCall;

  // Secondary entry point (for 2nd & subsequent TermsHash),
  // because token text has already been "interned" into
  // textStart, so we hash by textStart.  term vectors use
  // this API.
  private void add(int textStart, final int docID) throws IOException {
    int termID = bytesHash.addByPoolOffset(textStart); // TermsHashPerField的两个主要子类:TVConsumerPerField与FreqProxTermsWriterPerField
    if (termID >= 0) { // New posting                   // 都是由TVConsumer与FreqProxTermsWtr构造而来, 而他们用的是同一个byteBlockPool
      // First time we are seeing this token since we last
      // flushed the hash.
      initStreamSlices(termID, docID);
    } else {
      positionStreamSlice(termID, docID); // 不是第一次见, 添加
    }
  }

  /**
   * Called when we first encounter a new term. We must allocate slices to store the postings (vInt
   * compressed doc/freq/prox), and also the int pointers to where (in our ByteBlockPool storage)
   * the postings for this term begin.
   */
  private void initStreamSlices(int termID, int docID) throws IOException { // 初始化,
    // Init stream slices
    if (streamCount + intPool.intUpto > IntBlockPool.INT_BLOCK_SIZE) {
      // not enough space remaining in this buffer -- jump to next buffer and lose this remaining
      // piece
      intPool.nextBuffer();
    }// todo: 这个2我不明白;可能是因为每个TermsHashPerField都有streamCount参数，而且最多有两个TermsHashPerField?那调用两次，岂不是8?可能是因为Prox和Vector是共用的一个TermsHash，每人两个?
    // ( 2 * streamCount) * ByteBlockPool.FIRST_LEVEL_SIZE + bytePool.byteUpto > ByteBlockPool.BYTE_BLOCK_SIZE
    if (ByteBlockPool.BYTE_BLOCK_SIZE - bytePool.byteUpto // 这一层剩余的空间,都不能存放两个stream的第一个分片
        < (2 * streamCount) * ByteBlockPool.FIRST_LEVEL_SIZE) {
      // can we fit at least one byte per stream in the current buffer, if not allocate a new one
      bytePool.nextBuffer();
    }
    // intPool: _stream0  (_stream1）
    termStreamAddressBuffer = intPool.buffer;
    streamAddressOffset = intPool.intUpto; // stream count 最大是2， intUpto之后的两个int，分别存储着stream的内容在byte数组内的位置,见172行
    intPool.intUpto += streamCount; // advance the pool to reserve the N streams for this term
    //通过166行，这个termID的地址偏移, 可以获取到这个term的_stream0,_stream1的intaddr，通过intaddr在intpool里获取到slice在bytepool的开始地址
    postingsArray.addressOffset[termID] = streamAddressOffset + intPool.intOffset; // 数组的值, 可以找到对应stream在bytepool的位置
    // 上面一行,代表 termID 对应的 stream 的在bytepool中的地址 是 intPool的offset + 最开始的upto所在位置的数组值
    for (int i = 0; i < streamCount; i++) {
      // initialize each stream with a slice we start with ByteBlockPool.FIRST_LEVEL_SIZE)
      // and grow as we need more space. see ByteBlockPool.LEVEL_SIZE_ARRAY
      final int upto = bytePool.newSlice(ByteBlockPool.FIRST_LEVEL_SIZE);// 返回的是在bytePool分配的slice的起点在buffer哪里
      termStreamAddressBuffer[streamAddressOffset + i] = upto + bytePool.byteOffset; // 在_stream0,_steam里存储，见161行
    }//termID的倒排信息，在bytepool里的位置存在了A:termStreamAddressBuffer[streamAddressOffset],通过addressOffset[termId]得到的值,可以通过它到intPool里，获取A的值，在到bytePool开始读倒排信息
    postingsArray.byteStarts[termID] = termStreamAddressBuffer[streamAddressOffset];//这个term的以后的倒排信息(docid,
    // freq,甚至strem1的prox）在bytepool里的bytes开始的位置,设定好就不变了(其实也等于_stream0在bytepools的位置)
    newTerm(termID, docID);//而每个stream的可写位置,时刻变动,用intpool的偏移位置记录就好,从postingsArray.addressOffset[termID]获得
  }

  private boolean assertDocId(int docId) {
    assert docId >= lastDocID : "docID must be >= " + lastDocID + " but was: " + docId;
    lastDocID = docId;
    return true;
  }

  /**
   * Called once per inverted token. This is the primary entry point (for first TermsHash); postings
   * use this API.
   */
  void add(BytesRef termBytes, final int docID) throws IOException { // 测试方法: testAddAndUpdateTerm
    assert assertDocId(docID);
    // We are first in the chain so we must "intern" the
    // term text into textStart address
    // Get the text & hash of this term.
    int termID = bytesHash.add(termBytes);
    // System.out.println("add term=" + termBytesRef.utf8ToString() + " doc=" + docState.docID + "
    // termID=" + termID);
    if (termID >= 0) { // New posting // >=0 是第一次处理
      // Init stream slices
      initStreamSlices(termID, docID); // 新的term, 需要初始化各层级信息的stream. 并且每个stream都会存在于BytesBlockPool里,
    } else { //  所以调用的是BytesBlockPool的slice的方法
      termID = positionStreamSlice(termID, docID); // 小于0 , 不是第一次处理
    }
    if (doNextCall) { // doNextCall, 什么时候需要? termvector 构建的时候
      nextPerField.add(postingsArray.textStarts[termID], docID);
    }
  }

  private int positionStreamSlice(int termID, final int docID) throws IOException {
    termID = (-termID) - 1; // 真实的 termID
    int intStart = postingsArray.addressOffset[termID];
    termStreamAddressBuffer = intPool.buffers[intStart >> IntBlockPool.INT_BLOCK_SHIFT];
    streamAddressOffset = intStart & IntBlockPool.INT_BLOCK_MASK; // 得到在这个buffer里的位置
    addTerm(termID, docID); // addTerm的时候，从intPool里拿出bytepool的（对这个term的此stream）可写地址，
    return termID;//根据termStreamAddressBuffer[streamAddressOffset+stream]获得值:地址X,stream是想把这个term的信息写到哪个stream，
  } // bytepool[地址X],就是这个term的这个doc信息(freq,docid,pos等等)接下来应该在写入的地方（此stream申请出来的slice的空余byte游标）
  //有两跳跃，addressOffset->intpool->bytepool== 最终得到可以将此term的关于此docid的倒排信息写入的byte游标地址,区别于byteStarts,byteStarts存的是开始地址
  final void writeByte(int stream, byte b) {
    int streamAddress = streamAddressOffset + stream;
    int upto = termStreamAddressBuffer[streamAddress];
    byte[] bytes = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
    assert bytes != null;
    int offset = upto & ByteBlockPool.BYTE_BLOCK_MASK;
    if (bytes[offset] != 0) { // 要写入的byte 不等于0 ， 比如等于16，数据slice的哨兵，说明这个slice用完了， 分配个新的，slice
      // End of slice; allocate a new one // 且分配新的的过程中，[a][b][c][d][16] ||next slice->||
      offset = bytePool.allocSlice(bytes, offset);
      bytes = bytePool.buffer;
      termStreamAddressBuffer[streamAddress] = offset + bytePool.byteOffset;
    }
    bytes[offset] = b;
    (termStreamAddressBuffer[streamAddress])++;
  }

  final void writeBytes(int stream, byte[] b, int offset, int len) {
    final int end = offset + len;
    int streamAddress = streamAddressOffset + stream;
    int upto = termStreamAddressBuffer[streamAddress];
    byte[] slice = bytePool.buffers[upto >> ByteBlockPool.BYTE_BLOCK_SHIFT];
    assert slice != null;
    int sliceOffset = upto & ByteBlockPool.BYTE_BLOCK_MASK;

    while (slice[sliceOffset] == 0 && offset < end) {
      slice[sliceOffset++] = b[offset++];
      (termStreamAddressBuffer[streamAddress])++;
    }

    while (offset < end) {
      int offsetAndLength = bytePool.allocKnownSizeSlice(slice, sliceOffset);
      sliceOffset = offsetAndLength >> 8;
      int sliceLength = offsetAndLength & 0xff;
      slice = bytePool.buffer;
      int writeLength = Math.min(sliceLength - 1, end - offset);
      System.arraycopy(b, offset, slice, sliceOffset, writeLength);
      sliceOffset += writeLength;
      offset += writeLength;
      termStreamAddressBuffer[streamAddress] = sliceOffset + bytePool.byteOffset;
    }
  }

  final void writeVInt(int stream, int i) {
    assert stream < streamCount;
    while ((i & ~0x7F) != 0) {
      writeByte(stream, (byte) ((i & 0x7f) | 0x80));
      i >>>= 7;
    }
    writeByte(stream, (byte) i);
  }

  final TermsHashPerField getNextPerField() {
    return nextPerField;
  }

  final String getFieldName() {
    return fieldName;
  }

  private static final class PostingsBytesStartArray extends BytesStartArray {

    private final TermsHashPerField perField;
    private final Counter bytesUsed;

    private PostingsBytesStartArray(TermsHashPerField perField, Counter bytesUsed) {
      this.perField = perField;
      this.bytesUsed = bytesUsed;
    }

    @Override
    public int[] init() {
      if (perField.postingsArray == null) {
        perField.postingsArray = perField.createPostingsArray(2);
        perField.newPostingsArray();
        bytesUsed.addAndGet(perField.postingsArray.size * perField.postingsArray.bytesPerPosting());
      }
      return perField.postingsArray.textStarts;
    }

    @Override
    public int[] grow() {
      ParallelPostingsArray postingsArray = perField.postingsArray;
      final int oldSize = perField.postingsArray.size;
      postingsArray = perField.postingsArray = postingsArray.grow(); //
      perField.newPostingsArray();
      bytesUsed.addAndGet((postingsArray.bytesPerPosting() * (postingsArray.size - oldSize)));
      return postingsArray.textStarts;
    }

    @Override
    public int[] clear() {
      if (perField.postingsArray != null) {
        bytesUsed.addAndGet(
            -(perField.postingsArray.size * perField.postingsArray.bytesPerPosting()));
        perField.postingsArray = null;
        perField.newPostingsArray();
      }
      return null;
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }

  @Override
  public final int compareTo(TermsHashPerField other) {
    return fieldName.compareTo(other.fieldName);
  }

  /** Finish adding all instances of this field to the current document. */
  void finish() throws IOException {
    if (nextPerField != null) {
      nextPerField.finish();
    }
  }

  final int getNumTerms() {
    return bytesHash.size();
  }

  /**
   * Start adding a new field instance; first is true if this is the first time this field name was
   * seen in the document.
   */
  boolean start(IndexableField field, boolean first) { //FreqProx...PerField先调用这个方法的super, 然后调用TermVector...PerField
    if (nextPerField != null) { // 所以, 是 TermVector...PerField 先调用
      doNextCall = nextPerField.start(field, first);
    }
    return true;
  }

  /** Called when a term is seen for the first time. */
  abstract void newTerm(int termID, final int docID) throws IOException;

  /** Called when a previously seen term is seen again. */
  abstract void addTerm(int termID, final int docID) throws IOException;

  /** Called when the postings array is initialized or resized. */
  abstract void newPostingsArray();

  /** Creates a new postings array of the specified size. */
  abstract ParallelPostingsArray createPostingsArray(int size);
}
