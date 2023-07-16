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
package org.apache.lucene.codecs;

import java.io.IOException;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.MathUtil;

/**
 * This abstract class writes skip lists with multiple levels.
 *
 * <pre>
 * // 注意： 第0层是最底层，再往下就是包含所有的数据的数据层，而后往上每一层都/skipInterval，增加上层的节点
 * Example for skipInterval = 3:
 *                                                     c            (skip level 2)
 *                 c                 c                 c            (skip level 1)
 *     x     x     x     x     x     x     x     x     x     x      (skip level 0)
 * d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d d  (posting list)
 *     3     6     9     12    15    18    21    24    27    30     (df)
 *
 * d - document
 * x - skip data
 * c - skip data with child pointer
 * // level i 包含 level i-1 的节点 按照每skipInterval所生成的索引节点
 * Skip level i contains every skipInterval-th entry from skip level i-1.
 * Therefore the number of entries on level i is: floor(df / ((skipInterval ^ (i + 1))).
 *
 * Each skip entry on a level {@code i>0} contains a pointer to the corresponding skip entry in list i-1.
 * This guarantees a logarithmic amount of skips to find the target document.
 *
 * While this class takes care of writing the different skip levels,
 * subclasses must define the actual format of the skip data.
 * </pre>
 *
 * @lucene.experimental
 */
public abstract class MultiLevelSkipListWriter { // 子类实现就是：各种存储文件的自我实现;如写在text里的skiptext，写在lucene90里的
  /** number of levels in this skip list */
  protected final int numberOfSkipLevels; // 跳表的层数
  // org.apache.lucene.codecs.lucene90.ForUtil.BLOCK_SIZE = 128
  /** the skip interval in the list with level = 0 *///每处理skipInterval篇文档，就生成一个skipDatum，该值默认为128
  private final int skipInterval; // 每处理skipInterval个文档生成一个第0层跳表的节点，其实就是0层每个跳表节点包含的文档数

  /** skipInterval used for level &gt; 0 */ // 上下两个参数在lucene90里分别是128,8
  private final int skipMultiplier; // 从第0层开始，每层间隔skipMultiplier的节点就加入上一层, 加一个节点

  /** for every skip level a different buffer is used */
  private ByteBuffersDataOutput[] skipBuffer; // 缓存，每层都用一个不同的

  /** Creates a {@code MultiLevelSkipListWriter}. */
  protected MultiLevelSkipListWriter( //
      int skipInterval, int skipMultiplier, int maxSkipLevels, int df) {
    this.skipInterval = skipInterval;
    this.skipMultiplier = skipMultiplier;

    int numberOfSkipLevels; // 总层数
    // calculate the maximum number of skip levels for this document frequency
    if (df <= skipInterval) { // 都不能在第0层的跳表里生成一个节点， 那就一层把
      numberOfSkipLevels = 1;
    } else {//df/skipInterval表示最底层(第0层)有多少个节点,log(第0层的总节点个数，skipMultiplier)//就像是完全二叉树最后一层,就是logn向下取整,然后 + 1
      numberOfSkipLevels = 1 + MathUtil.log(df / skipInterval, skipMultiplier);
    }                           // 但是,这样的话,第0层岂不是只有1个节点

    // make sure it does not exceed maxSkipLevels
    if (numberOfSkipLevels > maxSkipLevels) {  // 当超过了最大的层数， 那我们就增大各层的节点个数就好，而不是叠层数，层数越多，性能也会受到影响
      numberOfSkipLevels = maxSkipLevels;      // 这也是一个trade off
    }
    this.numberOfSkipLevels = numberOfSkipLevels;
  }

  /**
   * Creates a {@code MultiLevelSkipListWriter}, where {@code skipInterval} and {@code
   * skipMultiplier} are the same.
   */
  protected MultiLevelSkipListWriter(int skipInterval, int maxSkipLevels, int df) {
    this(skipInterval, skipInterval, maxSkipLevels, df);
  }

  /** Allocates internal skip buffers. */ // 主要是为每一层建立一个缓存，用来存储每一层数据。最后对跳表的持久化就是持久化这些缓存。
  protected void init() {
    skipBuffer = new ByteBuffersDataOutput[numberOfSkipLevels];
    for (int i = 0; i < numberOfSkipLevels; i++) {
      skipBuffer[i] = ByteBuffersDataOutput.newResettableInstance();
    }
  }

  /** Creates new buffers or empties the existing ones */
  protected void resetSkip() {
    if (skipBuffer == null) {
      init();
    } else {
      for (int i = 0; i < skipBuffer.length; i++) {
        skipBuffer[i].reset();
      }
    }
  }

  /** // skipDatum
   * Subclasses must implement the actual skip data encoding in this method.
   *
   * @param level the level skip data shall be writing for
   * @param skipBuffer the skip buffer to write to
   */
  protected abstract void writeSkipData(int level, DataOutput skipBuffer) throws IOException;

  /**
   * Writes the current skip data to the buffers. The current document frequency determines the max
   * level is skip data is to be written to.
   * // 根据当前已经处理的文档数量，预先计算出将待写入SkipDatum信息的层数 // buffer下来
   * @param df the current document frequency
   * @throws IOException If an I/O error occurs
   */ // 每隔128个doc， 就会执行一次bufferSkip操作, 目的是把跳表写入buffers中 // df表示到目前为止的文档总数，根据df可以得到当前要生成的跳表节点最多可以到达第几层。
  public void bufferSkip(int df) throws IOException {//buffer是动词,skip是名词.把df个doc缓存下来,构建出skiplist.等到最后writerskip的时候才会写

    assert df % skipInterval == 0;
    int numLevels = 1;
    df /= skipInterval; // df表示最底层(第0层)有多少个节点
    // df 现在应该是 nodeIdIn0层
    // determine max level // 看看df个数据， 拓展到了第几层的第几个节点
    while ((df % skipMultiplier) == 0 && numLevels < numberOfSkipLevels) { // 为什么要 % == 0 todo? 是不是提前计算好的，只有满足条件的才调用
      numLevels++;                                                  // 在SimpleTextSkipWriter中，当%8的时候，才调用bufferSkip
      df /= skipMultiplier; // 每skipMultiplier个SkipDatum就在上一层生成一个SkipDatum
    }

    long childPointer = 0;
    //注意level的计算， 是目前为止的doc总数，所以，它buffer一个最底层的节点（block）的时候，会向上蔓延，来生成索引
    for (int level = 0; level < numLevels; level++) {
      writeSkipData(level, skipBuffer[level]); // 表示, 将level层的数据写到level层

      long newChildPointer = skipBuffer[level].size(); // 写入的数据的长度

      if (level != 0) {
        // store child pointers for all levels except the lowest // 从高层往下层的
        writeChildPointer(childPointer, skipBuffer[level]); // 将数据的长度写入,一个好的codec不会也写skipbuffer[level]的数据的
      }

      // remember the childPointer for the next level
      childPointer = newChildPointer;
    }
  }

  /**
   * Writes the buffered skip lists to the given output.
   *一个term一个跳表，当一个term在所有文档的倒排信息都处理完成之后，调用writeSkip持久化跳表，跳表数据持久化到doc索引文件中，因为跳表就是用来快速定位doc的位置的。
   * 持久化就是把每一层的缓存数据持久化，从最高层开始处理。
   * @param output the IndexOutput the skip lists shall be written to
   * @return the pointer the skip list starts
   */
  public long writeSkip(IndexOutput output) throws IOException { // 等于是持久化
    long skipPointer = output.getFilePointer();
    // System.out.println("skipper.writeSkip fp=" + skipPointer);
    if (skipBuffer == null || skipBuffer.length == 0) return skipPointer;

    for (int level = numberOfSkipLevels - 1; level > 0; level--) {
      long length = skipBuffer[level].size();
      if (length > 0) {
        writeLevelLength(length, output); // 先写 length
        skipBuffer[level].copyTo(output); // 再写 每一层的data,从最高层往下写(4层的length,4层的数据),(),(),(1层的length,1层的数据)(0层的数据)
      }
    }
    skipBuffer[0].copyTo(output);

    return skipPointer;
  }

  /**
   * Writes the length of a level to the given output.
   *
   * @param levelLength the length of a level
   * @param output the IndexOutput the length shall be written to
   */
  protected void writeLevelLength(long levelLength, IndexOutput output) throws IOException {
    output.writeVLong(levelLength);
  }

  /**
   * Writes the child pointer of a block to the given output.
   *
   * @param childPointer block of higher level point to the lower level
   * @param skipBuffer the skip buffer to write to
   */
  protected void writeChildPointer(long childPointer, DataOutput skipBuffer) throws IOException {
    skipBuffer.writeVLong(childPointer);
  }
}
