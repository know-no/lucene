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

import static org.apache.lucene.codecs.lucene90.ForUtil.BLOCK_SIZE;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.DOC_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.MAX_SKIP_LEVELS;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.PAY_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.POS_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.TERMS_CODEC;
import static org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.VERSION_CURRENT;

import java.io.IOException;
import org.apache.lucene.codecs.BlockTermState;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.PushPostingsWriterBase;
import org.apache.lucene.codecs.lucene90.Lucene90PostingsFormat.IntBlockTermState;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;

/**
 * Concrete class that writes docId(maybe frq,pos,offset,payloads) list with postings format.
 *
 * <p>Postings list for each term will be stored separately.
 *
 * @see Lucene90SkipWriter for details about skipping setting and postings layout.
 * @lucene.experimental
 */
public final class Lucene90PostingsWriter extends PushPostingsWriterBase { // .doc .pos  .pay   文件

  IndexOutput docOut;
  IndexOutput posOut;
  IndexOutput payOut;

  static final IntBlockTermState emptyState = new IntBlockTermState();
  IntBlockTermState lastState;

  // Holds starting file pointers for current term:
  private long docStartFP; // 当前term即将写的<docId，freq信息>在doc文件里的开始位置
  private long posStartFP; // 当前term在pos文件里的开始位置
  private long payStartFP; //  。。。。

  final long[] docDeltaBuffer;// 下标是docBufferUpto，获取的是此下标和前一个下标代表的docId的delta, buffer内（128）
  final long[] freqBuffer; // 往一个block内收集，下标是这个block内收集了多少个doc：docBufferUpto，值是freq
  private int docBufferUpto;//是postingWriter对某个term在一个block（128）累积写入的doc的数目统计，满足128就会清零
                            //切换到一个新的term，就会清零
  final long[] posDeltaBuffer;//差值存储pos，下标是出现的次序， // 下面的这些数组都不用清零，只需要游标posBufferUpto清0就可以
  final long[] payloadLengthBuffer;// 临时存储每个doc的每个pos下的payloads值的长度，值本身却在payloadBytes,下标是：this.payloadByteUpto
  final long[] offsetStartDeltaBuffer; // 差值存储， 每个位置的offset与前一个位置的offset的start的差值
  final long[] offsetLengthBuffer; // 差值存储，每个位置的offset的长度与前一个位置的length的差值
  private int posBufferUpto;//posDeltaBuffer[]的下标,每满128个就清0

  private byte[] payloadBytes; // payloadLengthBuffer存的是payloads的长度， 而它却存的是值本身
  private int payloadByteUpto; // 临时存储每个doc的每个pos下的payloads的数组：payloadLengthBuffer的下标

  private int lastBlockDocID; // 在写term的倒排信息过程中， 记录着上一个 doc block 中的最后一个doc的id
  private long lastBlockPosFP; // 上一个block(doc凑齐了128个）, 它们的pos信息在pos文件中的结束位置
  private long lastBlockPayFP; // 上一个block(doc凑齐了128个）, 它们的pay信息在pay文件中的结束位置
  private int lastBlockPosBufferUpto;
  private int lastBlockPayloadByteUpto;

  private int lastDocID; // 当前的term，在刷倒排信息进入文件，lastDocId，标识当前刷的是哪个doc的信息
  private int lastPosition; // 当前的term，在处理某个doc的时候，doc内的变量
  private int lastStartOffset; // 临时存储，pos的offset的start位置 , doc内的变量
  private int docCount; // 当前在写的term， 处理了多少个doc了

  private final PForUtil pforUtil;
  private final Lucene90SkipWriter skipWriter;

  private boolean fieldHasNorms;
  private NumericDocValues norms;
  private final CompetitiveImpactAccumulator competitiveFreqNormAccumulator =
      new CompetitiveImpactAccumulator();

  /** Creates a postings writer */
  public Lucene90PostingsWriter(SegmentWriteState state) throws IOException {

    String docFileName =
        IndexFileNames.segmentFileName(
            state.segmentInfo.name, state.segmentSuffix, Lucene90PostingsFormat.DOC_EXTENSION);
    docOut = state.directory.createOutput(docFileName, state.context);
    IndexOutput posOut = null;
    IndexOutput payOut = null;
    boolean success = false;
    try {
      CodecUtil.writeIndexHeader(
          docOut, DOC_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
      pforUtil = new PForUtil(new ForUtil());
      if (state.fieldInfos.hasProx()) { // PostingsWriter发现，如果Fields里有Field需要prox，还会初始化 .pos
        posDeltaBuffer = new long[BLOCK_SIZE];
        String posFileName =
            IndexFileNames.segmentFileName(
                state.segmentInfo.name, state.segmentSuffix, Lucene90PostingsFormat.POS_EXTENSION);
        posOut = state.directory.createOutput(posFileName, state.context);
        CodecUtil.writeIndexHeader(
            posOut, POS_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);

        if (state.fieldInfos.hasPayloads()) {
          payloadBytes = new byte[128];
          payloadLengthBuffer = new long[BLOCK_SIZE];
        } else {
          payloadBytes = null;
          payloadLengthBuffer = null;
        }

        if (state.fieldInfos.hasOffsets()) {
          offsetStartDeltaBuffer = new long[BLOCK_SIZE];
          offsetLengthBuffer = new long[BLOCK_SIZE];
        } else {
          offsetStartDeltaBuffer = null;
          offsetLengthBuffer = null;
        }

        if (state.fieldInfos.hasPayloads() || state.fieldInfos.hasOffsets()) {
          String payFileName = // PostingsWriter发现，如果Fields里有Field需要 payloads 或者 offsets 还会初始化 .pay
              IndexFileNames.segmentFileName(
                  state.segmentInfo.name,
                  state.segmentSuffix,
                  Lucene90PostingsFormat.PAY_EXTENSION);
          payOut = state.directory.createOutput(payFileName, state.context);
          CodecUtil.writeIndexHeader(
              payOut, PAY_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
        }
      } else {
        posDeltaBuffer = null;
        payloadLengthBuffer = null;
        offsetStartDeltaBuffer = null;
        offsetLengthBuffer = null;
        payloadBytes = null;
      }
      this.payOut = payOut;
      this.posOut = posOut;
      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
    }

    docDeltaBuffer = new long[BLOCK_SIZE];
    freqBuffer = new long[BLOCK_SIZE];

    // TODO: should we try skipping every 2/4 blocks...?
    skipWriter =
        new Lucene90SkipWriter(
            MAX_SKIP_LEVELS, BLOCK_SIZE, state.segmentInfo.maxDoc(), docOut, posOut, payOut);
  }

  @Override
  public IntBlockTermState newTermState() {
    return new IntBlockTermState();
  }

  @Override
  public void init(IndexOutput termsOut, SegmentWriteState state) throws IOException {
    CodecUtil.writeIndexHeader(
        termsOut, TERMS_CODEC, VERSION_CURRENT, state.segmentInfo.getId(), state.segmentSuffix);
    termsOut.writeVInt(BLOCK_SIZE);
  }

  @Override
  public void setField(FieldInfo fieldInfo) {
    super.setField(fieldInfo);
    skipWriter.setField(writePositions, writeOffsets, writePayloads);
    lastState = emptyState;
    fieldHasNorms = fieldInfo.hasNorms();
  }

  @Override
  public void startTerm(NumericDocValues norms) {
    docStartFP = docOut.getFilePointer();
    if (writePositions) {
      posStartFP = posOut.getFilePointer();
      if (writePayloads || writeOffsets) {
        payStartFP = payOut.getFilePointer();
      }
    }
    lastDocID = 0; // 开启了一个新的term， lastDocId设为0
    lastBlockDocID = -1; // 上一个block的docId，没有block则为-1,此刻开启了新的term，必然
    skipWriter.resetSkip();
    this.norms = norms;
    competitiveFreqNormAccumulator.clear();
  }

  @Override
  public void startDoc(int docID, int termDocFreq) throws IOException {
    // Have collected a block of docs, and get a new doc.// 已经收集了一个block的docs， 现在获得了新的doc
    // Should write skip data as well as postings list for // 为当前（刚才写完的）的block，写skip data 和 postings list
    // current block.
    if (lastBlockDocID != -1 && docBufferUpto == 0) { // != -1 表示：这不是一个新的term && ==0表示，当前的buffer里没有文档
      skipWriter.bufferSkip(//这两个条件加起来，就是说刚在这个term里收集了一个block的docs，现在要调用skipWriter，为这个block，写skip信息
          lastBlockDocID,
          competitiveFreqNormAccumulator,
          docCount,
          lastBlockPosFP,
          lastBlockPayFP,
          lastBlockPosBufferUpto,
          lastBlockPayloadByteUpto);
      competitiveFreqNormAccumulator.clear();
    }
    // 接下来都是预备这个docID的状态, 为接下里的倒排链写入准备
    final int docDelta = docID - lastDocID;

    if (docID < 0 || (docCount > 0 && docDelta <= 0)) {
      throw new CorruptIndexException(
          "docs out of order (" + docID + " <= " + lastDocID + " )", docOut);
    }

    docDeltaBuffer[docBufferUpto] = docDelta;
    if (writeFreqs) {
      freqBuffer[docBufferUpto] = termDocFreq;
    }

    docBufferUpto++;
    docCount++;

    if (docBufferUpto == BLOCK_SIZE) {
      pforUtil.encode(docDeltaBuffer, docOut); // 已经有128个了,把他们压缩一下，写入docOut，然后在下一把startDoc的时候，调用skipwriter写block
      if (writeFreqs) {
        pforUtil.encode(freqBuffer, docOut);
      }
      // NOTE: don't set docBufferUpto back to 0 here;
      // finishDoc will do so (because it needs to see that // finishDoc会帮助设为0
      // the block was filled so it can save skip data)
    }

    lastDocID = docID;
    lastPosition = 0;
    lastStartOffset = 0;

    long norm;
    if (fieldHasNorms) {
      boolean found = norms.advanceExact(docID);
      if (found == false) {
        // This can happen if indexing hits a problem after adding a doc to the
        // postings but before buffering the norm. Such documents are written
        // deleted and will go away on the first merge.
        norm = 1L;
      } else {
        norm = norms.longValue();
        assert norm != 0 : docID;
      }
    } else {
      norm = 1L;
    }

    competitiveFreqNormAccumulator.add(writeFreqs ? termDocFreq : 1, norm);
  }

  @Override
  public void addPosition(int position, BytesRef payload, int startOffset, int endOffset)
      throws IOException {
    if (position > IndexWriter.MAX_POSITION) {
      throw new CorruptIndexException(
          "position="
              + position
              + " is too large (> IndexWriter.MAX_POSITION="
              + IndexWriter.MAX_POSITION
              + ")",
          docOut);
    }
    if (position < 0) {
      throw new CorruptIndexException("position=" + position + " is < 0", docOut);
    }
    posDeltaBuffer[posBufferUpto] = position - lastPosition; // 差值存储
    if (writePayloads) {
      if (payload == null || payload.length == 0) {
        // no payload
        payloadLengthBuffer[posBufferUpto] = 0; // 第几个pos的payload
      } else {
        payloadLengthBuffer[posBufferUpto] = payload.length;
        if (payloadByteUpto + payload.length > payloadBytes.length) { // 扩容payloadBytes数组，原始是128个
          payloadBytes = ArrayUtil.grow(payloadBytes, payloadByteUpto + payload.length);
        } // 将payload拷贝进临时数组： payloadBytes
        System.arraycopy(
            payload.bytes, payload.offset, payloadBytes, payloadByteUpto, payload.length);
        payloadByteUpto += payload.length; // 增加payloadByteUpto
      }
    }

    if (writeOffsets) {
      assert startOffset >= lastStartOffset;
      assert endOffset >= startOffset;
      offsetStartDeltaBuffer[posBufferUpto] = startOffset - lastStartOffset;
      offsetLengthBuffer[posBufferUpto] = endOffset - startOffset;
      lastStartOffset = startOffset;
    }

    posBufferUpto++;
    lastPosition = position;
    if (posBufferUpto == BLOCK_SIZE) {// pos满128个, offset和payloads也就满128
      pforUtil.encode(posDeltaBuffer, posOut); // 满了以后，就写入到posOut

      if (writePayloads) {
        pforUtil.encode(payloadLengthBuffer, payOut);
        payOut.writeVInt(payloadByteUpto); // 先写,这个第128个payloads的在length数组里的下标
        payOut.writeBytes(payloadBytes, 0, payloadByteUpto);
        payloadByteUpto = 0;
      }
      if (writeOffsets) {
        pforUtil.encode(offsetStartDeltaBuffer, payOut);
        pforUtil.encode(offsetLengthBuffer, payOut);
      }
      posBufferUpto = 0; // 游标清零， 即可， 数组本身不用
    }
  }

  @Override
  public void finishDoc() throws IOException {
    // Since we don't know df for current term, we had to buffer
    // those skip data for each block, and when a new doc comes,
    // write them to skip file.
    if (docBufferUpto == BLOCK_SIZE) { // 对应 startDoc()方法里的247行; 即start的时候发现，写完了一个block
      lastBlockDocID = lastDocID; // 更新上一个block的BlockDocID
      if (posOut != null) {                        // 并且更新 这个block的最后一个doc的对应的pay，pos在文件里的位置
        if (payOut != null) {
          lastBlockPayFP = payOut.getFilePointer();// 更新上一个payloads block的指针
        }
        lastBlockPosFP = posOut.getFilePointer(); // 更新上一个 pos block的指针
        lastBlockPosBufferUpto = posBufferUpto; // 上个block完成的时候，它的pos信息在缓存数组里的位置，所以，其实不同doc的pos信息其实是会混在一个block里的
        lastBlockPayloadByteUpto = payloadByteUpto; // payloads信息也是
      }
      docBufferUpto = 0;
    }
  }

  /** Called when we are done adding docs to this term */
  @Override // 由此可见，对于一个term先写docId，freq，pos，pay等等信息，写完之后， 想.doc文件里写 skip list
  public void finishTerm(BlockTermState _state) throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    assert state.docFreq > 0;

    // TODO: wasteful we are counting this (counting # docs
    // for this term) in two places?
    assert state.docFreq == docCount : state.docFreq + " vs " + docCount;

    // docFreq == 1, don't write the single docid/freq to a separate file along with a pointer to
    // it. // 如果doc只有1， 则不单独写个文件
    final int singletonDocID;
    if (state.docFreq == 1) {
      // pulse the singleton docid into the term dictionary, freq is implicitly totalTermFreq
      singletonDocID = (int) docDeltaBuffer[0];
    } else {
      singletonDocID = -1;
      // vInt encode the remaining doc deltas and freqs: // 还剩下一些，没有凑足一个block的, 用vint的方式编码
      for (int i = 0; i < docBufferUpto; i++) {
        final int docDelta = (int) docDeltaBuffer[i];
        final int freq = (int) freqBuffer[i];
        if (!writeFreqs) {
          docOut.writeVInt(docDelta);
        } else if (freq == 1) {
          docOut.writeVInt((docDelta << 1) | 1);
        } else {
          docOut.writeVInt(docDelta << 1);
          docOut.writeVInt(freq);
        }
      }
    }

    final long lastPosBlockOffset;

    if (writePositions) { // totalTermFreq就是 total position， payloads，offsets
      // totalTermFreq is just total number of positions(or payloads, or offsets)
      // associated with current term.
      assert state.totalTermFreq != -1;
      if (state.totalTermFreq > BLOCK_SIZE) { // 超过一个block
        // record file offset for last pos in last block
        lastPosBlockOffset = posOut.getFilePointer() - posStartFP;
      } else {
        lastPosBlockOffset = -1;
      }
      if (posBufferUpto > 0) {
        // TODO: should we send offsets/payloads to
        // .pay...?  seems wasteful (have to store extra
        // vLong for low (< BLOCK_SIZE) DF terms = vast vast
        // majority)

        // vInt encode the remaining positions/payloads/offsets:
        int lastPayloadLength = -1; // force first payload length to be written
        int lastOffsetLength = -1; // force first offset length to be written
        int payloadBytesReadUpto = 0;
        for (int i = 0; i < posBufferUpto; i++) { // 剩下的pos s
          final int posDelta = (int) posDeltaBuffer[i];
          if (writePayloads) {
            final int payloadLength = (int) payloadLengthBuffer[i];
            if (payloadLength != lastPayloadLength) { //强制写下第一个，后续只有在出现不同的情况下才写
              lastPayloadLength = payloadLength;
              posOut.writeVInt((posDelta << 1) | 1);
              posOut.writeVInt(payloadLength);        // 411行数说的写，是什么， payloadLength
            } else {
              posOut.writeVInt(posDelta << 1);
            }

            if (payloadLength != 0) { // payload 存在， 则将其一并写入到pos文件里:从payloadBytes的payloadBytesReadUpto位置开始读
              posOut.writeBytes(payloadBytes, payloadBytesReadUpto, payloadLength);
              payloadBytesReadUpto += payloadLength;
            }
          } else {
            posOut.writeVInt(posDelta);
          }

          if (writeOffsets) {
            int delta = (int) offsetStartDeltaBuffer[i];
            int length = (int) offsetLengthBuffer[i];
            if (length == lastOffsetLength) {
              posOut.writeVInt(delta << 1); // 以低位0标识是否有新的length数据，为0则复用前面的就好了，不为0，则读一下
            } else {
              posOut.writeVInt(delta << 1 | 1);
              posOut.writeVInt(length);
              lastOffsetLength = length;
            }
          }
        }

        if (writePayloads) {
          assert payloadBytesReadUpto == payloadByteUpto;
          payloadByteUpto = 0;
        }
      }
    } else {
      lastPosBlockOffset = -1;
    }
    // 由此可见，对于一个term先写docId，freq，pos，pay等等信息，写完之后， 想.doc文件里写 skip list
    long skipOffset;
    if (docCount > BLOCK_SIZE) { // 判断总docCount是否大于BlockSize， 若大于则至少一个block说明肯定存在跳表，需要持久化跳表
      skipOffset = skipWriter.writeSkip(docOut) - docStartFP; // 把之前构建的跳表，写入docOut
    } else {
      skipOffset = -1;
    }
    // state中的这些信息就是term的元信息，会存储在term字典中
    state.docStartFP = docStartFP; // 更新IntBlockTermState的状态，这个term的倒排信息在文件里从docStarFP开始
    state.posStartFP = posStartFP; // same
    state.payStartFP = payStartFP; // same
    state.singletonDocID = singletonDocID; // 是否只有一个doc
    state.skipOffset = skipOffset; // 跳表数据在文件里的位置，offset， 相对于 docStartFP而言
    state.lastPosBlockOffset = lastPosBlockOffset; // 最后一个pos block在文件中的结束位置（也就是最后用vint压缩的开始位置）
    docBufferUpto = 0;
    posBufferUpto = 0;
    lastDocID = 0;
    docCount = 0;
  }

  @Override
  public void encodeTerm(
      DataOutput out, FieldInfo fieldInfo, BlockTermState _state, boolean absolute)
      throws IOException {
    IntBlockTermState state = (IntBlockTermState) _state;
    if (absolute) {
      lastState = emptyState;
      assert lastState.docStartFP == 0;
    }

    if (lastState.singletonDocID != -1
        && state.singletonDocID != -1
        && state.docStartFP == lastState.docStartFP) {
      // With runs of rare values such as ID fields, the increment of pointers in the docs file is
      // often 0.
      // Furthermore some ID schemes like auto-increment IDs or Flake IDs are monotonic, so we
      // encode the delta
      // between consecutive doc IDs to save space.
      final long delta = (long) state.singletonDocID - lastState.singletonDocID;
      out.writeVLong((BitUtil.zigZagEncode(delta) << 1) | 0x01);
    } else {
      out.writeVLong((state.docStartFP - lastState.docStartFP) << 1);
      if (state.singletonDocID != -1) {
        out.writeVInt(state.singletonDocID);
      }
    }

    if (writePositions) {
      out.writeVLong(state.posStartFP - lastState.posStartFP);
      if (writePayloads || writeOffsets) {
        out.writeVLong(state.payStartFP - lastState.payStartFP);
      }
    }
    if (writePositions) {
      if (state.lastPosBlockOffset != -1) {
        out.writeVLong(state.lastPosBlockOffset);
      }
    }
    if (state.skipOffset != -1) {
      out.writeVLong(state.skipOffset);
    }
    lastState = state;
  }

  @Override
  public void close() throws IOException {
    // TODO: add a finish() at least to PushBase? DV too...?
    boolean success = false;
    try {
      if (docOut != null) {
        CodecUtil.writeFooter(docOut);
      }
      if (posOut != null) {
        CodecUtil.writeFooter(posOut);
      }
      if (payOut != null) {
        CodecUtil.writeFooter(payOut);
      }
      success = true;
    } finally {
      if (success) {
        IOUtils.close(docOut, posOut, payOut);
      } else {
        IOUtils.closeWhileHandlingException(docOut, posOut, payOut);
      }
      docOut = posOut = payOut = null;
    }
  }
}
