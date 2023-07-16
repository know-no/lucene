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

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import org.apache.lucene.codecs.CompetitiveImpactAccumulator;
import org.apache.lucene.codecs.MultiLevelSkipListWriter;
import org.apache.lucene.index.Impact;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;

/**
 * Write skip lists with multiple levels, and support skip within block ints.
 *
 * <p>Assume that docFreq = 28, skipInterval = blockSize = 12
 *
 * <pre>
 *  |       block#0       | |      block#1        | |vInts|
 *  d d d d d d d d d d d d d d d d d d d d d d d d d d d d (posting list)
 *                          ^                       ^       (level 0 skip point)
 * </pre>
 * // 会忽略block#0的第一个doc，因为对于skip无用，并且最后一个vInts block也不用探查
 * <p>Note that skipWriter will ignore first document in block#0, since it is useless as a skip
 * point. Also, we'll never skip into the vInts block, only record skip data at the start its start
 * point(if it exist).
 * //对于每个skip point， 记录： 前一个位置的docId，比如对于pos：12，记录docID[11]  2. 它的关联文件的指针 3. 关联的信息 4 起始offset
 * <p>For each skip point, we will record: 1. docID in former position, i.e. for position 12, record
 * docID[11], etc. 2. its related file points(position, payload), 3. related numbers or
 * uptos(position, payload). 4. start offset.
 */
final class Lucene90SkipWriter extends MultiLevelSkipListWriter { // 是lucene90 的skip data 写入类,特点是：支持block ints 的跳跃
  private int[] lastSkipDoc; //下标是level， 值是level的前一个跳表节点的docID
  private long[] lastSkipDocPointer; //下标是level，值是level的前一个block的doc索引文件的结束位置，其实就是当前block的开始的位置
  private long[] lastSkipPosPointer; //下标是level，值是指定level的前一个block的pos索引文件的结束位置，其实就是当前block的开始的位置
  private long[] lastSkipPayPointer; //下标是level，值是指定level的前一个block的pay索引文件的结束位置，其实就是当前block的开始的位置
  private int[] lastPayloadByteUpto;// 下标是level，值是指定level的前一个block的payload缓存的结束位置，其实就是当前block的开始的位置

  private final IndexOutput docOut;
  private final IndexOutput posOut;
  private final IndexOutput payOut;

  private int curDoc; // 当前block的docID: 指的是block的最后
  private long curDocPointer; // 当前block在doc文件中的结束位置
  private long curPosPointer; // 当前block在pos文件中的结束位置
  private long curPayPointer; // 当前block在pay文件中的结束位置
  private int curPosBufferUpto;// 当前block在posBuffer中的结束位置
  private int curPayloadByteUpto; // 当前block在payBuffer中的结束位置
  private CompetitiveImpactAccumulator[] curCompetitiveFreqNorms; // 每一层的Impact计算结果
  private boolean fieldHasPositions;
  private boolean fieldHasOffsets;
  private boolean fieldHasPayloads;

  public Lucene90SkipWriter(
      int maxSkipLevels,
      int blockSize,
      int docCount,
      IndexOutput docOut,
      IndexOutput posOut,
      IndexOutput payOut) {
    super(blockSize, 8, maxSkipLevels, docCount);
    this.docOut = docOut;
    this.posOut = posOut;
    this.payOut = payOut;

    lastSkipDoc = new int[maxSkipLevels];
    lastSkipDocPointer = new long[maxSkipLevels];
    if (posOut != null) {
      lastSkipPosPointer = new long[maxSkipLevels];
      if (payOut != null) {
        lastSkipPayPointer = new long[maxSkipLevels];
      }
      lastPayloadByteUpto = new int[maxSkipLevels];
    }
    curCompetitiveFreqNorms = new CompetitiveImpactAccumulator[maxSkipLevels];
    for (int i = 0; i < maxSkipLevels; ++i) {
      curCompetitiveFreqNorms[i] = new CompetitiveImpactAccumulator();
    }
  }

  public void setField(
      boolean fieldHasPositions, boolean fieldHasOffsets, boolean fieldHasPayloads) {
    this.fieldHasPositions = fieldHasPositions;
    this.fieldHasOffsets = fieldHasOffsets;
    this.fieldHasPayloads = fieldHasPayloads;
  }

  // tricky: we only skip data for blocks (terms with more than 128 docs), but re-init'ing the
  // skipper
  // is pretty slow for rare terms in large segments as we have to fill O(log #docs in segment) of
  // junk.
  // this is the vast majority of terms (worst case: ID field or similar).  so in resetSkip() we
  // save
  // away the previous pointers, and lazy-init only if we need to buffer skip data for the term.
  private boolean initialized;
  long lastDocFP;
  long lastPosFP;
  long lastPayFP;

  @Override
  public void resetSkip() {
    lastDocFP = docOut.getFilePointer(); // 所谓last，则是当下的，即将开始的，可以写入的位置
    if (fieldHasPositions) {
      lastPosFP = posOut.getFilePointer();
      if (fieldHasOffsets || fieldHasPayloads) {
        lastPayFP = payOut.getFilePointer();
      }
    }
    if (initialized) {
      for (CompetitiveImpactAccumulator acc : curCompetitiveFreqNorms) {
        acc.clear();
      }
    }
    initialized = false;
  }

  private void initSkip() { // org.apache.lucene.codecs.lucene90.Lucene90PostingsWriter.startTerm开启一个新的term的时候才会重置
    if (!initialized) {
      super.resetSkip();
      Arrays.fill(lastSkipDoc, 0); // 置为0
      Arrays.fill(lastSkipDocPointer, lastDocFP); // 在reset的时候设置的，上一个block在doc文件中的结束位置。每一层的last信息都是这个值
      if (fieldHasPositions) {    // 在writeSkiData的时候，会计算delta
        Arrays.fill(lastSkipPosPointer, lastPosFP); // 同， 在reset的时候设置，
        if (fieldHasPayloads) {
          Arrays.fill(lastPayloadByteUpto, 0);
        }
        if (fieldHasOffsets || fieldHasPayloads) {
          Arrays.fill(lastSkipPayPointer, lastPayFP);
        }
      }
      // sets of competitive freq,norm pairs should be empty at this point
      assert Arrays.stream(curCompetitiveFreqNorms)
              .map(CompetitiveImpactAccumulator::getCompetitiveFreqNormPairs)
              .mapToInt(Collection::size)
              .sum()
          == 0;
      initialized = true;
    }
  }

  /** Sets the values for the current skip data. */
  public void bufferSkip( // buffer skip,在内存里生成一个block，以及它对应的索引信息：它最后一个doc的id，对应的pos信息在文件里的结束位置
      int doc, // 当前生成的这个block，block中的最后一个doc         // pay信息在pay文件里结束位置 // pos信息目前写到了缓存数组的哪里
      CompetitiveImpactAccumulator competitiveFreqNorms,
      int numDocs, // 共计多少个doc， 包括之前的， 是个总数
      long posFP,
      long payFP,
      int posBufferUpto,
      int payloadByteUpto)
      throws IOException {
    initSkip();
    this.curDoc = doc;
    this.curDocPointer = docOut.getFilePointer(); // 获取写完后的 .doc文件的游标
    this.curPosPointer = posFP;
    this.curPayPointer = payFP;
    this.curPosBufferUpto = posBufferUpto;
    this.curPayloadByteUpto = payloadByteUpto;
    this.curCompetitiveFreqNorms[0].addAll(competitiveFreqNorms);
    bufferSkip(numDocs);
  }

  private final ByteBuffersDataOutput freqNormOut = ByteBuffersDataOutput.newResettableInstance();

  @Override // 在bufferSkip中已经将一个block的索引信息（或者理解为一个节点）写入到某一层的skipBuffe里了
  protected void writeSkipData(int level, DataOutput skipBuffer) throws IOException {
//skipBuffer里存的是：docIdDelata,block在doc文件的地址delta,block在pos文件的地址delta,
    int delta = curDoc - lastSkipDoc[level]; // level层的前一个调表节点的docId， 计算delta如果是第一次写入就是当前curDoc

    skipBuffer.writeVInt(delta); // 写vint delta， 到此 层的buffer里
    lastSkipDoc[level] = curDoc; // 此层buffer的当前docId，修改此层
    // 每一level的writerSkipData在读取的时候，初始值是上一个term的最后一个block的结束位置(新term的第一个写入位置),之后便会被cur值取代
    skipBuffer.writeVLong(curDocPointer - lastSkipDocPointer[level]);//求取delta,所以skipBuffer里写入的一个节点的信息，是
    lastSkipDocPointer[level] = curDocPointer;        // 计算的delte可以理解为这个block的在doc里的数据的长度是多少

    if (fieldHasPositions) {

      skipBuffer.writeVLong(curPosPointer - lastSkipPosPointer[level]);
      lastSkipPosPointer[level] = curPosPointer;
      skipBuffer.writeVInt(curPosBufferUpto);

      if (fieldHasPayloads) {
        skipBuffer.writeVInt(curPayloadByteUpto);
      }

      if (fieldHasOffsets || fieldHasPayloads) {
        skipBuffer.writeVLong(curPayPointer - lastSkipPayPointer[level]);
        lastSkipPayPointer[level] = curPayPointer;
      }
    }

    CompetitiveImpactAccumulator competitiveFreqNorms = curCompetitiveFreqNorms[level];
    assert competitiveFreqNorms.getCompetitiveFreqNormPairs().size() > 0;
    if (level + 1 < numberOfSkipLevels) {
      curCompetitiveFreqNorms[level + 1].addAll(competitiveFreqNorms);
    }
    writeImpacts(competitiveFreqNorms, freqNormOut);
    skipBuffer.writeVInt(Math.toIntExact(freqNormOut.size()));
    freqNormOut.copyTo(skipBuffer);
    freqNormOut.reset();
    competitiveFreqNorms.clear();
  }

  static void writeImpacts(CompetitiveImpactAccumulator acc, DataOutput out) throws IOException {
    Collection<Impact> impacts = acc.getCompetitiveFreqNormPairs();
    Impact previous = new Impact(0, 0);
    for (Impact impact : impacts) {
      assert impact.freq > previous.freq;
      assert Long.compareUnsigned(impact.norm, previous.norm) > 0;
      int freqDelta = impact.freq - previous.freq - 1;
      long normDelta = impact.norm - previous.norm - 1;
      if (normDelta == 0) {
        // most of time, norm only increases by 1, so we can fold everything in a single byte
        out.writeVInt(freqDelta << 1);
      } else {
        out.writeVInt((freqDelta << 1) | 1);
        out.writeZLong(normDelta);
      }
      previous = impact;
    }
  }
}
