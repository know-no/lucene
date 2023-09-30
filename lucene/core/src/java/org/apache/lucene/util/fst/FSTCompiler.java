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
package org.apache.lucene.util.fst;

import java.io.IOException;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.FST.INPUT_TYPE; // javadoc

// TODO: could we somehow stream an FST to disk while we
// build it?

/** // 构造一个最小化FST, 从一个预排序的terms集，其中term带有output（任意output，只要满足：add，subtract, min）即util.fst.Outputs
 * Builds a minimal FST (maps an IntsRef term to an arbitrary output) from pre-sorted terms with
 * outputs. The FST becomes an FSA if you use NoOutputs. The FST is written on-the-fly into a // 压缩数组，这样能持久化存
 * compact serialized format byte array, which can be saved to / loaded from a Directory or used
 * directly for traversal. The FST is always finite (no cycles). // 有限无环
 *
 * <p>NOTE: The algorithm is described at
 * http://citeseerx.ist.psu.edu/viewdoc/summary?doi=10.1.1.24.3698
 *
 * <p>The parameterized type T is the output type. See the subclasses of {@link Outputs}.
 *
 * <p>FSTs larger than 2.1GB are now possible (as of Lucene 4.2). FSTs containing more than 2.1B
 * nodes are also now possible, however they cannot be packed.
 *
 * @lucene.experimental
 */
public class FSTCompiler<T> { // 构建FST的入口类, 不再是使用FST#Builder类了

  static final float DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR = 1f;

  private final NodeHash<T> dedupHash; // 为了避免重复创建Node // 会返回 id
  final FST<T> fst;
  private final T NO_OUTPUT;

  // private static final boolean DEBUG = true;

  // simplistic pruning: we prune node (and all following
  // nodes) if less than this number of terms go through it:
  private final int minSuffixCount1; // 怎么剪枝

  // better pruning: we prune node (and all following
  // nodes) if the prior node has less than this number of
  // terms go through it:
  private final int minSuffixCount2;

  private final boolean doShareNonSingletonNodes;
  private final int shareMaxTailLength;

  private final IntsRefBuilder lastInput = new IntsRefBuilder(); // 上一次处理的term值

  // NOTE: cutting this over to ArrayList instead loses ~6%
  // in build performance on 9.8M Wikipedia terms; so we
  // left this as an array:
  // current "frontier"
  private UnCompiledNode<T>[] frontier; // 存放还未处理的 节点

  // Used for the BIT_TARGET_NEXT optimization (whereby
  // instead of storing the address of the target node for
  // a given arc, we mark a single bit noting that the next
  // node in the byte[] is the target node):
  long lastFrozenNode;//构建FST时，每次处理完一个节点，就会返回一个long类型的值，如果是 终止节点那么返回固定值 -1，否则返回在current[]数组中的下标值。


  // Reused temporarily while building the FST:
  int[] numBytesPerArc = new int[4];
  int[] numLabelBytesPerArc = new int[numBytesPerArc.length];
  final FixedLengthArcsBuffer fixedLengthArcsBuffer = new FixedLengthArcsBuffer();

  long arcCount;
  long nodeCount;
  long binarySearchNodeCount;
  long directAddressingNodeCount;

  final boolean allowFixedLengthArcs;
  final float directAddressingMaxOversizingFactor;
  long directAddressingExpansionCredit;

  final BytesStore bytes;

  /**
   * Instantiates an FST/FSA builder with default settings and pruning options turned off. For more
   * tuning and tweaking, see {@link Builder}.
   */
  public FSTCompiler(FST.INPUT_TYPE inputType, Outputs<T> outputs) {
    this(inputType, 0, 0, true, true, Integer.MAX_VALUE, outputs, true, 15, 1f);
  }

  private FSTCompiler(
      FST.INPUT_TYPE inputType,
      int minSuffixCount1,
      int minSuffixCount2,
      boolean doShareSuffix,
      boolean doShareNonSingletonNodes,
      int shareMaxTailLength,
      Outputs<T> outputs,
      boolean allowFixedLengthArcs,
      int bytesPageBits,
      float directAddressingMaxOversizingFactor) {
    this.minSuffixCount1 = minSuffixCount1;
    this.minSuffixCount2 = minSuffixCount2;
    this.doShareNonSingletonNodes = doShareNonSingletonNodes;
    this.shareMaxTailLength = shareMaxTailLength;
    this.allowFixedLengthArcs = allowFixedLengthArcs;
    this.directAddressingMaxOversizingFactor = directAddressingMaxOversizingFactor;
    fst = new FST<>(inputType, outputs, bytesPageBits); // 新建FST对象，把里面的第一个byte写为0，标识为末尾，是因为
    bytes = fst.bytes;  // 读取的时候，是从后往前读的，读到0就知道到头了
    assert bytes != null;
    if (doShareSuffix) {
      dedupHash = new NodeHash<>(fst, bytes.getReverseReader(false));
    } else {
      dedupHash = null;
    }
    NO_OUTPUT = outputs.getNoOutput();

    @SuppressWarnings({"rawtypes", "unchecked"})
    final UnCompiledNode<T>[] f = (UnCompiledNode<T>[]) new UnCompiledNode[10];
    frontier = f; // 初始一个10大小的数组，存放尚未处理的节点的数据
    for (int idx = 0; idx < frontier.length; idx++) { // 先放一些空的
      frontier[idx] = new UnCompiledNode<>(this, idx);
    }
  }

  /**
   * Fluent-style constructor for FST {@link FSTCompiler}.
   *
   * <p>Creates an FST/FSA builder with all the possible tuning and construction tweaks. Read
   * parameter documentation carefully.
   */ // 构建FSTCompiler的建造者类, 实际的FST构建还是交给了FSTCompiler类
  public static class Builder<T> {

    private final INPUT_TYPE inputType;
    private final Outputs<T> outputs;
    private int minSuffixCount1;
    private int minSuffixCount2;
    private boolean shouldShareSuffix = true;
    private boolean shouldShareNonSingletonNodes = true;
    private int shareMaxTailLength = Integer.MAX_VALUE;
    private boolean allowFixedLengthArcs = true;
    private int bytesPageBits = 15;
    private float directAddressingMaxOversizingFactor = DIRECT_ADDRESSING_MAX_OVERSIZING_FACTOR;

    /**
     * @param inputType The input type (transition labels). Can be anything from {@link INPUT_TYPE}
     *     enumeration. Shorter types will consume less memory. Strings (character sequences) are
     *     represented as {@link INPUT_TYPE#BYTE4} (full unicode codepoints).
     * @param outputs The output type for each input sequence. Applies only if building an FST. For
     *     FSA, use {@link NoOutputs#getSingleton()} and {@link NoOutputs#getNoOutput()} as the
     *     singleton output object.
     */
    public Builder(FST.INPUT_TYPE inputType, Outputs<T> outputs) {
      this.inputType = inputType;
      this.outputs = outputs;
    }

    /**
     * If pruning the input graph during construction, this threshold is used for telling if a node
     * is kept or pruned. If transition_count(node) &gt;= minSuffixCount1, the node is kept.
     *
     * <p>Default = 0.
     */
    public Builder<T> minSuffixCount1(int minSuffixCount1) {
      this.minSuffixCount1 = minSuffixCount1;
      return this;
    }

    /**
     * Better pruning: we prune node (and all following nodes) if the prior node has less than this
     * number of terms go through it.
     *
     * <p>Default = 0.
     */
    public Builder<T> minSuffixCount2(int minSuffixCount2) {
      this.minSuffixCount2 = minSuffixCount2;
      return this;
    }

    /**
     * If {@code true}, the shared suffixes will be compacted into unique paths. This requires an
     * additional RAM-intensive hash map for lookups in memory. Setting this parameter to {@code
     * false} creates a single suffix path for all input sequences. This will result in a larger
     * FST, but requires substantially less memory and CPU during building.
     *
     * <p>Default = {@code true}.
     */
    public Builder<T> shouldShareSuffix(boolean shouldShareSuffix) {
      this.shouldShareSuffix = shouldShareSuffix;
      return this;
    }

    /**
     * Only used if {@code shouldShareSuffix} is true. Set this to true to ensure FST is fully
     * minimal, at cost of more CPU and more RAM during building.
     *
     * <p>Default = {@code true}.
     */
    public Builder<T> shouldShareNonSingletonNodes(boolean shouldShareNonSingletonNodes) {
      this.shouldShareNonSingletonNodes = shouldShareNonSingletonNodes;
      return this;
    }

    /**
     * Only used if {@code shouldShareSuffix} is true. Set this to Integer.MAX_VALUE to ensure FST
     * is fully minimal, at cost of more CPU and more RAM during building.
     *
     * <p>Default = {@link Integer#MAX_VALUE}.
     */
    public Builder<T> shareMaxTailLength(int shareMaxTailLength) {
      this.shareMaxTailLength = shareMaxTailLength;
      return this;
    }

    /**
     * Pass {@code false} to disable the fixed length arc optimization (binary search or direct
     * addressing) while building the FST; this will make the resulting FST smaller but slower to
     * traverse.
     *
     * <p>Default = {@code true}.
     */
    public Builder<T> allowFixedLengthArcs(boolean allowFixedLengthArcs) {
      this.allowFixedLengthArcs = allowFixedLengthArcs;
      return this;
    }

    /**
     * How many bits wide to make each byte[] block in the BytesStore; if you know the FST will be
     * large then make this larger. For example 15 bits = 32768 byte pages.
     *
     * <p>Default = 15.
     */
    public Builder<T> bytesPageBits(int bytesPageBits) {
      this.bytesPageBits = bytesPageBits;
      return this;
    }

    /**
     * Overrides the default the maximum oversizing of fixed array allowed to enable direct
     * addressing of arcs instead of binary search.
     *
     * <p>Setting this factor to a negative value (e.g. -1) effectively disables direct addressing,
     * only binary search nodes will be created.
     *
     * <p>This factor does not determine whether to encode a node with a list of variable length
     * arcs or with fixed length arcs. It only determines the effective encoding of a node that is
     * already known to be encoded with fixed length arcs.
     *
     * <p>Default = 1.
     */
    public Builder<T> directAddressingMaxOversizingFactor(float factor) {
      this.directAddressingMaxOversizingFactor = factor;
      return this;
    }

    /** Creates a new {@link FSTCompiler}. */
    public FSTCompiler<T> build() {
      FSTCompiler<T> fstCompiler =
          new FSTCompiler<>(
              inputType,
              minSuffixCount1,
              minSuffixCount2,
              shouldShareSuffix,
              shouldShareNonSingletonNodes,
              shareMaxTailLength,
              outputs,
              allowFixedLengthArcs,
              bytesPageBits,
              directAddressingMaxOversizingFactor);
      return fstCompiler;
    }
  }

  public float getDirectAddressingMaxOversizingFactor() {
    return directAddressingMaxOversizingFactor;
  }

  public long getTermCount() {
    return frontier[0].inputCount;
  }

  public long getNodeCount() {
    // 1+ in order to count the -1 implicit final node
    return 1 + nodeCount;
  }

  public long getArcCount() {
    return arcCount;
  }

  public long getMappedStateCount() {
    return dedupHash == null ? 0 : nodeCount;
  }

  private CompiledNode compileNode(UnCompiledNode<T> nodeIn, int tailLength) throws IOException {
    final long node;
    long bytesPosStart = bytes.getPosition(); // 当前FST的序列化存储的写入游标
    if (dedupHash != null // 判断关于是否要共享节点，决定了是否使用共享后缀
        && (doShareNonSingletonNodes || nodeIn.numArcs <= 1) // /*doShareNonSingletonNodes表示是否需要共享节点*/
        && tailLength <= shareMaxTailLength/*如果节点只有一个出边*/) { // 在配置的最长公共共享后缀范围之内
      if (nodeIn.numArcs == 0) { // 如果节点没有出边，一般是final节点
        node = fst.addNode(this, nodeIn);// 真正序列化是通过 fst.addNode，返回的node是节点在fst中flag的位置
        lastFrozenNode = node;
      } else { // dedupHash会判断节点是否已经序列化过， 如果序列化过了，则直接复用就好了.否则使用fst.addNode
        node = dedupHash.add(this, nodeIn); // 共享后缀的实现
      }
    } else {
      node = fst.addNode(this, nodeIn);
    }
    assert node != -2;

    long bytesPosEnd = bytes.getPosition(); // 序列化之后， bytes内的可写位置
    if (bytesPosEnd != bytesPosStart) { // 不相等，说明序列了一个新的node
      // The FST added a new node:
      assert bytesPosEnd > bytesPosStart;
      lastFrozenNode = node;
    }

    nodeIn.clear(); // 内容已经被序列化了， 不必保存了

    final CompiledNode fn = new CompiledNode();
    fn.node = node; // 编译好的node的id是多少
    return fn;
  }
  //prefixLenPlus1 = 前缀的长度 + 1, 方法的含义是：把上一个term的在frontier里的与当前的term不共享的后缀node冰冻，因为frontier里0是root，所以可以理解为+1是为了因为root
  // 占了数组的第一个位置
  private void freezeTail(int prefixLenPlus1) throws IOException { //
    // System.out.println("  compileTail " + prefixLenPlus1); // root是第0个节点，root是最后进行序列化的，所以最多序列化下标1节点
    final int downTo = Math.max(1, prefixLenPlus1); // 1, idx>=downTo，
    for (int idx = lastInput.length(); idx >= downTo; idx--) { // 关于剪枝， 虽然看不懂，但是luene的生产代码里
      // 从后往前冰冻， 目的是从后往前写入持久，读的时候就是正常序            // minSuffixCount1_2都是0， 所以不会走到这些逻辑
      boolean doPrune = false; // 和减枝有关
      boolean doCompile = false;
      // 获取两个节点，从后往前来
      final UnCompiledNode<T> node = frontier[idx];
      final UnCompiledNode<T> parent = frontier[idx - 1];
      // todo 剪枝
      if (node.inputCount < minSuffixCount1) { // 关于 减枝. 如果节点的路过值，小于最小的后缀设置
        doPrune = true;  // 如果Node的出边个数小于minSuffixCount1，会被删除  todo ? 为啥
        doCompile = true;
      } else if (idx > prefixLenPlus1) {
        // prune if parent's inputCount is less than suffixMinCount2
        if (parent.inputCount < minSuffixCount2 // 如果Node的父节点出边个数小于minSuffixCount2，会被删除
            || (minSuffixCount2 == 1 && parent.inputCount == 1 && idx > 1)) {
          // my parent, about to be compiled, doesn't make the cut, so
          // I'm definitely pruned

          // if minSuffixCount2 is 1, we keep only up
          // until the 'distinguished edge', ie we keep only the
          // 'divergent' part of the FST. if my parent, about to be
          // compiled, has inputCount 1 then we are already past the
          // distinguished edge.  NOTE: this only works if
          // the FST outputs are not "compressible" (simple
          // ords ARE compressible).
          doPrune = true;
        } else {
          // my parent, about to be compiled, does make the cut, so
          // I'm definitely not pruned
          doPrune = false;
        }
        doCompile = true;
      } else {
        // if pruning is disabled (count is 0) we can always
        // compile current node
        doCompile = minSuffixCount2 == 0;
      }
      // 因为暂时minSuffixCount都是0， 所以doPrune都会是false
      // System.out.println("    label=" + ((char) lastInput.ints[lastInput.offset+idx-1]) + " idx="
      // + idx + " inputCount=" + frontier[idx].inputCount + " doCompile=" + doCompile + " doPrune="
      // + doPrune);
      // 不会执行 minSuffixCount是0
      if (node.inputCount < minSuffixCount2
          || (minSuffixCount2 == 1 && node.inputCount == 1 && idx > 1)) {
        // drop all arcs
        for (int arcIdx = 0; arcIdx < node.numArcs; arcIdx++) {
          @SuppressWarnings({"rawtypes", "unchecked"})
          final UnCompiledNode<T> target = (UnCompiledNode<T>) node.arcs[arcIdx].target;
          target.clear();
        }
        node.numArcs = 0;
      }
      // 不会执行
      if (doPrune) {
        // this node doesn't make it -- deref it
        node.clear();
        parent.deleteLast(lastInput.intAt(idx - 1), node);
      } else {
        // 不会执行
        if (minSuffixCount2 != 0) {
          compileAllTargets(node, lastInput.length() - idx);
        }
        final T nextFinalOutput = node.output; // 获取node的output，如果是那种边不能挂值的情况

        // We "fake" the node as being final if it has no
        // outgoing arcs; in theory we could leave it
        // as non-final (the FST can represent this), but
        // FSTEnum, Util, etc., have trouble w/ non-final
        // dead-end states:
        final boolean isFinal = node.isFinal || node.numArcs == 0;

        if (doCompile) {// 重点关注这个，先把节点序列化好，然后再用CompiledNode替换UnCompiledNode
          // this node makes it and we now compile it.  first,
          // compile any targets that were previously
          // undecided:
          parent.replaceLast( //更新父亲节点的最后一条出边指向此节点，且此节点会被compile
              lastInput.intAt(idx - 1),
              compileNode(node, 1 + lastInput.length() - idx), // 此节点以及其往后的节点的长度
              nextFinalOutput,
              isFinal);
        } else {
          // replaceLast just to install
          // nextFinalOutput/isFinal onto the arc
          parent.replaceLast(lastInput.intAt(idx - 1), node, nextFinalOutput, isFinal);
          // this node will stay in play for now, since we are
          // undecided on whether to prune it.  later, it
          // will be either compiled or pruned, so we must
          // allocate a new node:
          frontier[idx] = new UnCompiledNode<>(this, idx);
        }
      }
    }
  }

  // for debugging
  /*
  private String toString(BytesRef b) {
    try {
      return b.utf8ToString() + " " + b;
    } catch (Throwable t) {
      return b.toString();
    }
  }
  */

  /**
   * Add the next input/output pair. The provided input must be sorted after the previous one
   * according to {@link IntsRef#compareTo}. It's also OK to add the same input twice in a row with
   * different outputs, as long as {@link Outputs} implements the {@link Outputs#merge} method. Note
   * that input is fully consumed after this method is returned (so caller is free to reuse), but
   * output is not. So if your outputs are changeable (eg {@link ByteSequenceOutputs} or {@link
   * IntSequenceOutputs}) then you cannot reuse across calls.
   */ // 添加一个term，两个参数：前者是可重用的，后者会被持有状态，不能重用
  public void add(IntsRef input, T output) throws IOException {
    /*
    if (DEBUG) {
      BytesRef b = new BytesRef(input.length);
      for(int x=0;x<input.length;x++) {
        b.bytes[x] = (byte) input.ints[x];
      }
      b.length = input.length;
      if (output == NO_OUTPUT) {
        System.out.println("\nFST ADD: input=" + toString(b) + " " + b);
      } else {
        System.out.println("\nFST ADD: input=" + toString(b) + " " + b + " output=" + fst.outputs.outputToString(output));
      }
    }
    */
    // 计算公共前缀 -》 判断是否有有节点不会变化，如果有则调用frezeTail 3 将arc：对应的节点写入frontier数组
    // De-dup NO_OUTPUT since it must be a singleton:
    if (output.equals(NO_OUTPUT)) {
      output = NO_OUTPUT;
    }

    assert lastInput.length() == 0 || input.compareTo(lastInput.get()) >= 0
        : "inputs are added out of order lastInput=" + lastInput.get() + " vs input=" + input;
    assert validOutput(output);

    // System.out.println("\nadd: " + input);
    if (input.length == 0) { // 特殊情况，空的输入
      // empty input: only allowed as first input.  we have
      // to special case this because the packed FST
      // format cannot represent the empty input since
      // 'finalness' is stored on the incoming arc, not on
      // the node
      frontier[0].inputCount++; // frontier[0]是根节点
      frontier[0].isFinal = true;
      fst.setEmptyOutput(output);
      return;
    }

    // compare shared prefix length
    int pos1 = 0;            // 遍历路径的游标
    int pos2 = input.offset; // 上一次插入，即上一个字典序的值，和当前插入的值，获得最小长度，防止越界
    final int pos1Stop = Math.min(lastInput.length(), input.length);
    while (true) {
      frontier[pos1].inputCount++; // 路径上的节点， 还未冰冻的节点，其路过值加加
      // System.out.println("  incr " + pos1 + " ct=" + frontier[pos1].inputCount + " n=" +
      // frontier[pos1]);
      if (pos1 >= pos1Stop || lastInput.intAt(pos1) != input.ints[pos2]) {
        break; // 当三种情况： 某一个走完了  或者不是字符不相同了
      }
      pos1++;
      pos2++;
    }
    final int prefixLenPlus1 = pos1 + 1; // 得到前一个的term，与当下的这个的共同前缀的长度 + 1

    if (frontier.length < input.length + 1) { // 这个term比较长，需要把 frontier扩容一下, 至少是这个term的长度+1,因为 frontier[0]是根节点
      final UnCompiledNode<T>[] next = ArrayUtil.grow(frontier, input.length + 1);
      for (int idx = frontier.length; idx < next.length; idx++) {
        next[idx] = new UnCompiledNode<>(this, idx); // 且都使用UnCompiledNode来暂时填充
      }
      frontier = next;
    }

    // minimize/compile states from previous input's
    // orphan'd suffix  // 冰冻前一个term在共同长度之后的节点
    freezeTail(prefixLenPlus1); // mop                                      0 -m-> 1 -o-> 2 -p-> 3
                                // moth  共同前缀是mo， 则冰冻前一个mop的p节点, 0 -m-> 1 -o-> 2 -t-> 3 -h-> 4
    // init tail states for current input // 非公共前缀部分加入frontier，这里就保证的前缀是共享的，只有一份
    for (int idx = prefixLenPlus1; idx <= input.length; idx++) {//frontier[prefixLenPlus1]是第一个不同字符的边指向的节点，在上例子中就是：节点3
      frontier[idx - 1].addArc(input.ints[input.offset + idx - 1], frontier[idx]); // 添加转化边,构建转移
      frontier[idx].inputCount++;
    }
    // 得到这个term的最后一个节点;  在保证输入有序的前提下，下面这个逻辑的if永远是true; 很容易理解，因为是在判断长度
    final UnCompiledNode<T> lastNode = frontier[input.length]; // prefixLenPlus1 = 共同前缀下标 + 1
    if (lastInput.length() != input.length || prefixLenPlus1 != input.length + 1) {
      lastNode.isFinal = true;
      lastNode.output = NO_OUTPUT;
    }

    // push conflicting outputs forward, only as far as
    // needed   //  从除了root外的第一个节点开始， 获取 父节点  以及 本节点
    for (int idx = 1; idx < prefixLenPlus1; idx++) { // 处理共同的前缀,发现冲突，重新分配output值
      final UnCompiledNode<T> node = frontier[idx];
      final UnCompiledNode<T> parentNode = frontier[idx - 1];
      // 获取父节点的最后一个出边的输出：在调用的此刻，还是上一个term的值
      final T lastOutput = parentNode.getLastOutput(input.ints[input.offset + idx - 1]);
      assert validOutput(lastOutput);

      final T commonOutputPrefix;
      final T wordSuffix;

      if (lastOutput != NO_OUTPUT) {
        commonOutputPrefix = fst.outputs.common(output, lastOutput); // 上个term留下的值与本次要写的值的 common值，对于字符串是共同前缀，对于数字，就是最小值
        assert validOutput(commonOutputPrefix);
        wordSuffix = fst.outputs.subtract(lastOutput, commonOutputPrefix);//前者减去后者,因为commonOutputPrefix一定小于等于output, lastOutput中的最小的那个
        assert validOutput(wordSuffix); // 计算出的值，就是要重新分配的值
        parentNode.setLastOutput(input.ints[input.offset + idx - 1], commonOutputPrefix); // 父节点更新最后的那个出边，到共同前缀
        node.prependOutput(wordSuffix); // 子节点则把父节点刚才卸下的值，往后更新,更新它的每个出边，如果没有出边了，就写到node本身的output去
      } else {
        commonOutputPrefix = wordSuffix = NO_OUTPUT;
      }

      output = fst.outputs.subtract(output, commonOutputPrefix); // 更新output的剩余值
      assert validOutput(output);
    }
    // 长度相同， 并且共同前缀的长度 == input.length // todo 为什么会这样，相同的term，可能存在吗
    if (lastInput.length() == input.length && prefixLenPlus1 == 1 + input.length) {
      // same input more than 1 time in a row, mapping to
      // multiple outputs // 结果做merge，默认的没有实现merge方法
      lastNode.output = fst.outputs.merge(lastNode.output, output);
    } else {
      // this new arc is private to this new input; set its
      // arc output to the leftover output:
      frontier[prefixLenPlus1 - 1].setLastOutput(  // 剩下的输出值都放在第一个新增arc上
          input.ints[input.offset + prefixLenPlus1 - 1], output);
    }

    // save last input
    lastInput.copyInts(input);

    // System.out.println("  count[0]=" + frontier[0].inputCount);
  }

  private boolean validOutput(T output) {
    return output == NO_OUTPUT || !output.equals(NO_OUTPUT);
  }

  /** Returns final FST. NOTE: this will return null if nothing is accepted by the FST. */
  public FST<T> compile() throws IOException { // 将root节点编译， 并且序列化

    final UnCompiledNode<T> root = frontier[0]; // root节点

    // minimize nodes in the last word's suffix
    freezeTail(0); // 冰冻除了root之外的所有节点
    if (root.inputCount < minSuffixCount1 // 剪枝相关，忽略
        || root.inputCount < minSuffixCount2
        || root.numArcs == 0) {
      if (fst.emptyOutput == null) {
        return null;
      } else if (minSuffixCount1 > 0 || minSuffixCount2 > 0) {
        // empty string got pruned
        return null;
      }
    } else {
      if (minSuffixCount2 != 0) {
        compileAllTargets(root, lastInput.length());
      }
    }
    // if (DEBUG) System.out.println("  builder.finish root.isFinal=" + root.isFinal + "
    // root.output=" + root.output);
    fst.finish(compileNode(root, lastInput.length()).node); // 最终完成构建

    return fst;
  }

  private void compileAllTargets(UnCompiledNode<T> node, int tailLength) throws IOException {
    for (int arcIdx = 0; arcIdx < node.numArcs; arcIdx++) {
      final Arc<T> arc = node.arcs[arcIdx];
      if (!arc.target.isCompiled()) {
        // not yet compiled
        @SuppressWarnings({"rawtypes", "unchecked"})
        final UnCompiledNode<T> n = (UnCompiledNode<T>) arc.target;
        if (n.numArcs == 0) {
          // System.out.println("seg=" + segment + "        FORCE final arc=" + (char) arc.label);
          arc.isFinal = n.isFinal = true;
        }
        arc.target = compileNode(n, tailLength - 1);
      }
    }
  }

  /** Expert: holds a pending (seen but not yet serialized) arc. */
  static class Arc<T> {
    int label; // really an "unsigned" byte //   当前边上的值，如msb/10, 这个边是m，则label是108，,108是m的unicode 码点
    Node target; // 指向下一个节点
    boolean isFinal; // Arc的output，表示当前Arc上有输出，isFinal表示Arc已经是终止Arc，其target是-1 (即终止节点，统一的特殊终止节点）
    T output;// 存储的是键值对的value值，类型是T（可以add，可以subtract，可以prefix）），如当前msb/10, 则可能是10，取决于当前的构建状态
    T nextFinalOutput;//nextFinalOutput就是这个arc指向的node的是final的时候，等于node的output，它只对以那个node为终止节点的查找才有用，区别于arc的output
  }

  // NOTE: not many instances of Node or CompiledNode are in
  // memory while the FST is being built; it's only the
  // current "frontier":

  interface Node {
    boolean isCompiled(); // 只有一个方法，如果是。。就。。
  }

  public long fstRamBytesUsed() {
    return fst.ramBytesUsed();
  }

  static final class CompiledNode implements Node {
    long node;// 节点的编号，直接理解成是节点在fst中的起始位置
    // 它后面的arc的信息已经被写入到current[]数组中. // 可以根据node标号，通过某种方式再找到它的值
    @Override
    public boolean isCompiled() {
      return true;
    }
  }

  /** Expert: holds a pending (seen but not yet serialized) Node. */
  static final class UnCompiledNode<T> implements Node {
    final FSTCompiler<T> owner; // 向外绑定
    int numArcs; // 会变化的，当时这个节点有多少个出边; 节点中可以包含多个arc，并且arc的信息未存放到current[]数组中。
    Arc<T>[] arcs; // 按照字典序来存放 出边, 容量会大于numArcs，后者是当前有多少个
    // TODO: instead of recording isFinal/output on the
    // node, maybe we should use -1 arc to mean "end" (like
    // we do when reading the FST).  Would simplify much
    // code here...           // finalOutput ： 使用 isFinal/output 来记录的值
    T output; // output 即节点附带的值，节点附带的值是有值不能存在我边上的情况: https://juejin.cn/post/7136100457399156766#heading-2
    boolean isFinal;//是否是终止节点,arc与node都有output和isFinal两个字段，Arc的output，表示当前Arc上有输出，isFinal表示Arc已经是终止Arc，其target是-1,不可以继续深度遍历。Node的isFinal表示从Node出发的Arc数量是0，当Node是Final Node时，output才有值。
    long inputCount; // 经过这个节点的路径个数，

    /** This node's depth, starting from the automaton root. */
    final int depth; // 节点距离 root 的距离

    /**
     * @param depth The node's depth starting from the automaton root. Needed for LUCENE-2934 (node
     *     expansion based on conditions other than the fanout size).
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    UnCompiledNode(FSTCompiler<T> owner, int depth) {
      this.owner = owner;
      arcs = (Arc<T>[]) new Arc[1];
      arcs[0] = new Arc<>();
      output = owner.NO_OUTPUT;
      this.depth = depth;
    }

    @Override
    public boolean isCompiled() {
      return false;
    }

    void clear() {
      numArcs = 0;
      isFinal = false;
      output = owner.NO_OUTPUT;
      inputCount = 0;

      // We don't clear the depth here because it never changes
      // for nodes on the frontier (even when reused).
    }

    T getLastOutput(int labelToMatch) {
      assert numArcs > 0;
      assert arcs[numArcs - 1].label == labelToMatch;
      return arcs[numArcs - 1].output; // 返回最后一个 出边 的附加值
    }
    // 新增一条出边，label出边的输入（即转换条件），target：arc的目标节点
    void addArc(int label, Node target) {
      assert label >= 0;
      assert numArcs == 0 || label > arcs[numArcs - 1].label // 可能是第一个出边，或者比前一个出边的字典序大
          : "arc[numArcs-1].label="
              + arcs[numArcs - 1].label
              + " new label="
              + label
              + " numArcs="
              + numArcs;
      if (numArcs == arcs.length) { // arcs里的位置不够用了
        final Arc<T>[] newArcs = ArrayUtil.grow(arcs);
        for (int arcIdx = numArcs; arcIdx < newArcs.length; arcIdx++) {
          newArcs[arcIdx] = new Arc<>(); // 并且给新的slots，默认赋予空Arc，不默认null
        }
        arcs = newArcs;
      }
      final Arc<T> arc = arcs[numArcs++];
      arc.label = label;
      arc.target = target;
      arc.output = arc.nextFinalOutput = owner.NO_OUTPUT;
      arc.isFinal = false;
    }
    // 更新字典序的最后一个出边: labelToMatch边本身的转换值，target，是否是最后output等
    void replaceLast(int labelToMatch, Node target, T nextFinalOutput, boolean isFinal) {
      assert numArcs > 0;
      final Arc<T> arc = arcs[numArcs - 1];
      assert arc.label == labelToMatch : "arc.label=" + arc.label + " vs " + labelToMatch;
      arc.target = target;
      // assert target.node != -2;
      arc.nextFinalOutput = nextFinalOutput;
      arc.isFinal = isFinal;
    }

    void deleteLast(int label, Node target) {
      assert numArcs > 0;
      assert label == arcs[numArcs - 1].label;
      assert target == arcs[numArcs - 1].target;
      numArcs--;
    }
    // 更新最后一条出边的output： lambelToMatch也是做个校验
    void setLastOutput(int labelToMatch, T newOutput) {
      assert owner.validOutput(newOutput);
      assert numArcs > 0;
      final Arc<T> arc = arcs[numArcs - 1];
      assert arc.label == labelToMatch;
      arc.output = newOutput;
    }
    // 更新所有出边的  output:  为它执行加的操作
    // pushes an output prefix forward onto all arcs
    void prependOutput(T outputPrefix) {
      assert owner.validOutput(outputPrefix);

      for (int arcIdx = 0; arcIdx < numArcs; arcIdx++) {
        arcs[arcIdx].output = owner.fst.outputs.add(outputPrefix, arcs[arcIdx].output);
        assert owner.validOutput(arcs[arcIdx].output);
      }// 如果是终止节点 , 这个节点可能存了一些output,如
      // dog: 3  dogs: 2 这种情况，dog是dogs的前缀，并且输出比dogs大，共享前缀输出是2，剩下的1需要保存在节点中
      if (isFinal) {//这里的if就是处理上面描述的这种情况
        output = owner.fst.outputs.add(outputPrefix, output);
        assert owner.validOutput(output);
      }
    }
  }

  /**
   * Reusable buffer for building nodes with fixed length arcs (binary search or direct addressing).
   */
  static class FixedLengthArcsBuffer {

    // Initial capacity is the max length required for the header of a node with fixed length arcs:
    // header(byte) + numArcs(vint) + numBytes(vint)
    private byte[] bytes = new byte[11];
    private final ByteArrayDataOutput bado = new ByteArrayDataOutput(bytes);

    /** Ensures the capacity of the internal byte array. Enlarges it if needed. */
    FixedLengthArcsBuffer ensureCapacity(int capacity) {
      if (bytes.length < capacity) {
        bytes = new byte[ArrayUtil.oversize(capacity, Byte.BYTES)];
        bado.reset(bytes);
      }
      return this;
    }

    FixedLengthArcsBuffer resetPosition() {
      bado.reset(bytes);
      return this;
    }

    FixedLengthArcsBuffer writeByte(byte b) {
      bado.writeByte(b);
      return this;
    }

    FixedLengthArcsBuffer writeVInt(int i) {
      try {
        bado.writeVInt(i);
      } catch (IOException e) { // Never thrown.
        throw new RuntimeException(e);
      }
      return this;
    }

    int getPosition() {
      return bado.getPosition();
    }

    /** Gets the internal byte array. */
    byte[] getBytes() {
      return bytes;
    }
  }
}
