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
package org.apache.lucene.util;

import static org.apache.lucene.util.RamUsageEstimator.NUM_BYTES_OBJECT_REF;

import java.util.Arrays;
import java.util.List;

/**
 * Class that Posting and PostingVector use to write byte streams into shared fixed-size byte[]
 * arrays. The idea is to allocate slices of increasing lengths For example, the first slice is 5
 * bytes, the next slice is 14, etc. We start by writing our bytes into the first 5 bytes. When we
 * hit the end of the slice, we allocate the next slice and then write the address of the new slice
 * into the last 4 bytes of the previous slice (the "forwarding address").
 *
 * <p>Each slice is filled with 0's initially, and we mark the end with a non-zero byte. This way
 * the methods that are writing into the slice don't need to record its length and instead allocate
 * a new slice once they hit a non-zero byte.
 *
 * @lucene.internal
 */
public final class ByteBlockPool implements Accountable {
  private static final long BASE_RAM_BYTES =
      RamUsageEstimator.shallowSizeOfInstance(ByteBlockPool.class);

  public static final int BYTE_BLOCK_SHIFT = 15;
  public static final int BYTE_BLOCK_SIZE = 1 << BYTE_BLOCK_SHIFT; // buffers的一个buffer的长度,有多少个byte
  public static final int BYTE_BLOCK_MASK = BYTE_BLOCK_SIZE - 1;

  /** Abstract class for allocating and freeing byte blocks. */
  public abstract static class Allocator {
    protected final int blockSize;

    protected Allocator(int blockSize) {
      this.blockSize = blockSize;
    }

    public abstract void recycleByteBlocks(byte[][] blocks, int start, int end);

    public void recycleByteBlocks(List<byte[]> blocks) {
      final byte[][] b = blocks.toArray(new byte[blocks.size()][]);
      recycleByteBlocks(b, 0, b.length);
    }

    public byte[] getByteBlock() {
      return new byte[blockSize];
    }
  }

  /** A simple {@link Allocator} that never recycles. */
  public static final class DirectAllocator extends Allocator {

    public DirectAllocator() {
      this(BYTE_BLOCK_SIZE);
    }

    public DirectAllocator(int blockSize) {
      super(blockSize);
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {}
  }

  /** A simple {@link Allocator} that never recycles, but tracks how much total RAM is in use. */
  public static class DirectTrackingAllocator extends Allocator {
    private final Counter bytesUsed;

    public DirectTrackingAllocator(Counter bytesUsed) {
      this(BYTE_BLOCK_SIZE, bytesUsed);
    }

    public DirectTrackingAllocator(int blockSize, Counter bytesUsed) {
      super(blockSize);
      this.bytesUsed = bytesUsed;
    }

    @Override
    public byte[] getByteBlock() {
      bytesUsed.addAndGet(blockSize);
      return new byte[blockSize];
    }

    @Override
    public void recycleByteBlocks(byte[][] blocks, int start, int end) {
      bytesUsed.addAndGet(-((end - start) * blockSize));
      for (int i = start; i < end; i++) {
        blocks[i] = null;
      }
    }
  }
  ;

  /**
   * array of buffers currently used in the pool. Buffers are allocated if needed don't modify this
   * outside of this class.
   */
  public byte[][] buffers = new byte[10][];

  /** index into the buffers array pointing to the current buffer used as the head */
  private int bufferUpto = -1; // Which buffer we are upto
  /** Where we are in head buffer */
  public int byteUpto = BYTE_BLOCK_SIZE; // the position is empty, 可以写

  /** Current head buffer */
  public byte[] buffer;
  /** Current head offset */ //当前使用的 buffer的开始位置 在二维数组中的 位置
  public int byteOffset = -BYTE_BLOCK_SIZE; // 初始是 - 的, 是因为使用的时候, 会先加上 block size

  private final Allocator allocator;

  public ByteBlockPool(Allocator allocator) {
    this.allocator = allocator;
  }

  /**
   * Resets the pool to its initial state reusing the first buffer and fills all buffers with <code>
   * 0</code> bytes before they reused or passed to {@link Allocator#recycleByteBlocks(byte[][],
   * int, int)}. Calling {@link ByteBlockPool#nextBuffer()} is not needed after reset.
   */
  public void reset() {
    reset(true, true);
  }

  /**
   * Expert: Resets the pool to its initial state reusing the first buffer. Calling {@link
   * ByteBlockPool#nextBuffer()} is not needed after reset. // 因为 resuing the first buffer
   *
   * @param zeroFillBuffers if <code>true</code> the buffers are filled with <code>0</code>. This
   *     should be set to <code>true</code> if this pool is used with slices.
   * @param reuseFirst if <code>true</code> the first buffer will be reused and calling {@link
   *     ByteBlockPool#nextBuffer()} is not needed after reset iff the block pool was used before
   *     ie. {@link ByteBlockPool#nextBuffer()} was called before.
   */
  public void reset(boolean zeroFillBuffers, boolean reuseFirst) {
    if (bufferUpto != -1) {
      // We allocated at least one buffer

      if (zeroFillBuffers) {
        for (int i = 0; i < bufferUpto; i++) {
          // Fully zero fill buffers that we fully used
          Arrays.fill(buffers[i], (byte) 0);
        }
        // Partial zero fill the final buffer
        Arrays.fill(buffers[bufferUpto], 0, byteUpto, (byte) 0);
      }

      if (bufferUpto > 0 || !reuseFirst) {
        final int offset = reuseFirst ? 1 : 0;
        // Recycle all but the first buffer
        allocator.recycleByteBlocks(buffers, offset, 1 + bufferUpto);
        Arrays.fill(buffers, offset, 1 + bufferUpto, null);
      }
      if (reuseFirst) {
        // Re-use the first buffer
        bufferUpto = 0;
        byteUpto = 0;
        byteOffset = 0;
        buffer = buffers[0];
      } else {
        bufferUpto = -1;
        byteUpto = BYTE_BLOCK_SIZE;
        byteOffset = -BYTE_BLOCK_SIZE;
        buffer = null;
      }
    }
  }

  /**
   * Advances the pool to its next buffer. This method should be called once after the constructor
   * to initialize the pool. In contrast to the constructor a {@link ByteBlockPool#reset()} call
   * will advance the pool to its first buffer immediately.
   */
  public void nextBuffer() {
    if (1 + bufferUpto == buffers.length) {
      byte[][] newBuffers =
          new byte[ArrayUtil.oversize(buffers.length + 1, NUM_BYTES_OBJECT_REF)][];
      System.arraycopy(buffers, 0, newBuffers, 0, buffers.length);
      buffers = newBuffers; //这个拷贝是很快的, 是因为值拷贝了一维数组的引用,快速放到目标数组上
    }
    buffer = buffers[1 + bufferUpto] = allocator.getByteBlock();
    bufferUpto++;

    byteUpto = 0;
    byteOffset += BYTE_BLOCK_SIZE; // 一般都是立刻调用, 所以byteOffset在初始化后,第一次赋值是0
  }
  // Slice 是在多线程下, 一种快速分配写空间的方法
  /**
   * Allocates a new slice with the given size. // 但是只会在第一次分配数据源的时候调用, 后面的size由
   *      // NEXT_LEVEL_ARRAY  和 LEVEL_SIZE_ARRAY 控制
   * @see ByteBlockPool#FIRST_LEVEL_SIZE
   */
  public int newSlice(final int size) { // 分配一个新的分片
    if (byteUpto > BYTE_BLOCK_SIZE - size) nextBuffer(); // byteUpto + size > BYTE_BLOCK_SIZE
    final int upto = byteUpto;
    byteUpto += size;
    buffer[byteUpto - 1] = 16; // 16是哨兵  哨兵是 16 | level , 在这里level是0, 16=1 0000, (0|1 0000)还是16，直接写入
    return upto; // buffer[upto] = 0
  }

  // Size of each slice.  These arrays should be at most 16
  // elements (index is encoded with 4 bits).  First array
  // is just a compact way to encode X+1 with a max.  Second
  // array is the length of each slice, ie first slice is 5
  // bytes, next slice is 14 bytes, etc.

  /**
   * An array holding the offset into the {@link ByteBlockPool#LEVEL_SIZE_ARRAY} to quickly navigate
   * to the next slice level.
   */ // 可以用作 LEVEL_SIZE_ARRAY的小标.看起来只有11层，实际上最后由两个9，所以第9层往后分配的都是 200 个byte的slice，牛逼
  public static final int[] NEXT_LEVEL_ARRAY = {1, 2, 3, 4, 5, 6, 7, 8, 9, 9};//之所以要(16 | newLevel);可能是想

  /** An array holding the level sizes for byte slices. */ // 5是0层
  public static final int[] LEVEL_SIZE_ARRAY = {5, 14, 20, 30, 40, 40, 80, 80, 120, 200};
                                              //0  1   2   3    4   5  6   7   8     9
  /**
   * The first level size for new slices
   *
   * @see ByteBlockPool#newSlice(int)
   */
  public static final int FIRST_LEVEL_SIZE = LEVEL_SIZE_ARRAY[0];

  /**
   * Creates a new byte slice with the given starting size and returns the slices offset in the
   * pool.
   */ // 从 buffer[][] 里切出一片, 从调用上可知:参数slice就是 buffer[][]里的一个buffer, 而upto
  public int allocSlice(final byte[] slice, final int upto) { // 则是要分配的位置
    return allocKnownSizeSlice(slice, upto) >> 8;
  }

  /**
   * Create a new byte slice with the given starting size return the slice offset in the pool and
   * length. The lower 8 bits of the returned int represent the length of the slice, and the upper
   * 24 bits represent the offset.
   */
  public int allocKnownSizeSlice(final byte[] slice, final int upto) {
    final int level = slice[upto] & 15; // 15 = 1111 // slice uptp 就是flag的位置,  如果是最开始的level, 1111 & 0 = 0
    final int newLevel = NEXT_LEVEL_ARRAY[level]; // 0 的下一个level, 以0为下标,在数组NEXT_LEVEL_ARRAY里就得到了
    final int newSize = LEVEL_SIZE_ARRAY[newLevel]; // 得到新的size

    // Maybe allocate another block
    if (byteUpto > BYTE_BLOCK_SIZE - newSize) { // byteUpto + newSize > SIZE
      nextBuffer(); // byteUpto的值会变化
    }

    final int newUpto = byteUpto; // 其实byteUpto，如果经过了258行的nextBuffer(),那他就是0了
    final int offset = newUpto + byteOffset;
    byteUpto += newSize;
    // 获取当前upto前面三个字节，也就是哨兵前面的3个字节。这是为了把这部分空间留出来(3+1)，设置下一个slice的地址。注意是小端读取，通过与运算把哨兵字节（upto指向的字节）的过滤了。
    // Copy forward the past 3 bytes (which we are about to overwrite with the forwarding address).
    // We actually copy 4 bytes at once since VarHandles make it cheap.
    int past3Bytes = ((int) BitUtil.VH_LE_INT.get(slice, upto - 3)) & 0xFFFFFF; //  小端读取，通过与运算把哨兵字节（upto指向的字节）的过滤了。
    // Ensure we're not changing the content of `buffer` by setting 4 bytes instead of 3. This
    // should never happen since the next `newSize` bytes must be equal to 0.
    assert buffer[newUpto + 3] == 0;// 假定newUpto+3的是0,因为要写入一个Int(虽然原本是想写3byte) // 把最后三个bytes 复制过来的目的是什么 ?
    BitUtil.VH_LE_INT.set(buffer, newUpto, past3Bytes); // 是因为会在原来的slice的后3+1位置写入新建的slice的地址,会覆盖这三个,所以提前复制过来

    // Write forwarding address at end of last slice:
    BitUtil.VH_LE_INT.set(slice, upto - 3, offset);//向上一层的slice的末尾写入下一层的地址
    //  X X X Y, 写入的是一个int, 会覆盖upto的byte, 这个byte也没用了；todo 但是是怎么判断出这个slice的最后四个代表的是地址呢
    // Write new level: //todo 完全通过NEXT_LEVEL_ARRAY 和 LEVEL_SIZE_ARRAY吗? 从开始的时候，依次认为是递增的level
    buffer[byteUpto - 1] = (byte) (16 | newLevel); // 在设置下一个level的时候，设置本level。newSize的长度不仅包含data,好包含了level
    // 不过当新的slice的数据写到 byteUpto-1的时候，也会干掉这个哨兵，用于新的扩容的
    return ((newUpto + 3) << 8) | (newSize - 3);// newUpto是新slice的第一个地址，然后第一第二第三都写入了前一个slice的值，见271行，
  }// 第四个则写入的是0. 所以 newUpto + 3是此新的slice的当下可以使用的写入位置。至于<<8, 是因为想返回告诉上层，这个新分配的slice还剩下多少
   // 可以写入的空间,所以把newSize-3也返回了， 然后newSize最大是200，即小于2^8
  /**
   * Fill the provided {@link BytesRef} with the bytes at the specified offset/length slice. This
   * will avoid copying the bytes, if the slice fits into a single block; otherwise, it uses the
   * provided {@link BytesRefBuilder} to copy bytes over.
   */ // 从pool的offset开始，取length个字节到result中。 如果offset+length刚好在一个block内, 则不复制.否则复制
  void setBytesRef(BytesRefBuilder builder, BytesRef result, long offset, int length) {
    result.length = length;

    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    byte[] buffer = buffers[bufferIndex];
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    if (pos + length <= BYTE_BLOCK_SIZE) {
      // common case where the slice lives in a single block: just reference the buffer directly
      // without copying
      result.bytes = buffer;
      result.offset = pos;
    } else {
      // uncommon case: the slice spans at least 2 blocks, so we must copy the bytes:
      builder.grow(length);
      result.bytes = builder.get().bytes;
      result.offset = 0;
      readBytes(offset, result.bytes, 0, length);
    }
  }

  // Fill in a BytesRef from term's length & bytes encoded in
  // byte block // 从pool的textStart位置读取term，读取的时候先读term的长度，term的长度是1字节或者两字节，要根据当前的标志位来判断
  public void setBytesRef(BytesRef term, int textStart) {
    final byte[] bytes = term.bytes = buffers[textStart >> BYTE_BLOCK_SHIFT];
    int pos = textStart & BYTE_BLOCK_MASK;
    if ((bytes[pos] & 0x80) == 0) { // byte & 1000 0000 , byte的最高位如果是0  其实也好理解, term的长度可能是用vint写入的,并且1是延续位
      // length is 1 byte     term的长度使用1byte编码的                           // 0是结束标识
      term.length = bytes[pos];
      term.offset = pos + 1;
    } else { // term的长度是用2byte编码的 // 即 0xxxxxxx 1xxxxxxx, 在writeVInt的时候，先写了1xxxxxxxx，然后写0xxxxxxx。
      // length is 2 bytes        // 大端方式读取, 第一个byte是1标识开头的byte, 剩下一个byte是0开头的
      term.length = ((short) BitUtil.VH_BE_SHORT.get(bytes, pos)) & 0x7FFF; // 0111 1111 1111 1111
      term.offset = pos + 2;
    }
    assert term.length >= 0;
  }

  /** Appends the bytes in the provided {@link BytesRef} at the current position. */
  public void append(final BytesRef bytes) {
    int bytesLeft = bytes.length;
    int offset = bytes.offset;
    while (bytesLeft > 0) {
      int bufferLeft = BYTE_BLOCK_SIZE - byteUpto;
      if (bytesLeft < bufferLeft) {
        // fits within current buffer
        System.arraycopy(bytes.bytes, offset, buffer, byteUpto, bytesLeft);
        byteUpto += bytesLeft;
        break;
      } else { // bufferLeft <= bytesLeft
        // fill up this buffer and move to next one
        if (bufferLeft > 0) {
          System.arraycopy(bytes.bytes, offset, buffer, byteUpto, bufferLeft);
        }
        nextBuffer();
        bytesLeft -= bufferLeft;
        offset += bufferLeft;
      }
    }
  }

  /**
   * Reads bytes out of the pool starting at the given offset with the given length into the given
   * byte array at offset <code>off</code>.
   * // 从pool的offset处读取bytesLength个字节到bytes中bytesOffset的位置。可以处理跨buffer的情况。
   * <p>Note: this method allows to copy across block boundaries.
   */
  public void readBytes(final long offset, final byte[] bytes, int bytesOffset, int bytesLength) {
    int bytesLeft = bytesLength;
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT);
    int pos = (int) (offset & BYTE_BLOCK_MASK);
    while (bytesLeft > 0) {
      byte[] buffer = buffers[bufferIndex++];
      int chunk = Math.min(bytesLeft, BYTE_BLOCK_SIZE - pos);
      System.arraycopy(buffer, pos, bytes, bytesOffset, chunk);
      bytesOffset += chunk;
      bytesLeft -= chunk;
      pos = 0;
    }
  }

  /**
   * Set the given {@link BytesRef} so that its content is equal to the {@code ref.length} bytes
   * starting at {@code offset}. Most of the time this method will set pointers to internal
   * data-structures. However, in case a value crosses a boundary, a fresh copy will be returned. On
   * the contrary to {@link #setBytesRef(BytesRef, int)}, this does not expect the length to be
   * encoded with the data.
   */
  public void setRawBytesRef(BytesRef ref, final long offset) {
    int bufferIndex = (int) (offset >> BYTE_BLOCK_SHIFT); // 定位到要读取的buffer
    int pos = (int) (offset & BYTE_BLOCK_MASK); // buffer内的具体byte的下标
    if (pos + ref.length <= BYTE_BLOCK_SIZE) { // 如果要读取的数据不跨buffer， 则设置地址就好，不进行底层复制
      ref.bytes = buffers[bufferIndex];
      ref.offset = pos;
    } else {                                   // 跨行了，在底层进行复制
      ref.bytes = new byte[ref.length];
      ref.offset = 0;
      readBytes(offset, ref.bytes, 0, ref.length);
    }
  }

  @Override
  public long ramBytesUsed() {
    long size = BASE_RAM_BYTES;
    size += RamUsageEstimator.sizeOfObject(buffer);
    size += RamUsageEstimator.shallowSizeOf(buffers);
    for (byte[] buf : buffers) {
      if (buf == buffer) {
        continue;
      }
      size += RamUsageEstimator.sizeOfObject(buf);
    }
    return size;
  }
}
