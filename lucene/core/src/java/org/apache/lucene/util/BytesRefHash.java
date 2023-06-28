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

import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_MASK;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SHIFT;
import static org.apache.lucene.util.ByteBlockPool.BYTE_BLOCK_SIZE;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.lucene.util.ByteBlockPool.DirectAllocator;

/**
 * {@link BytesRefHash} is a special purpose hash-map like data-structure optimized for {@link
 * BytesRef} instances. BytesRefHash maintains mappings of byte arrays to ids
 * (Map&lt;BytesRef,int&gt;) storing the hashed bytes efficiently in continuous storage. The mapping
 * to the id is encapsulated inside {@link BytesRefHash} and is guaranteed to be increased for each
 * added {@link BytesRef}.
 *
 * <p>Note: The maximum capacity {@link BytesRef} instance passed to {@link #add(BytesRef)} must not
 * be longer than {@link ByteBlockPool#BYTE_BLOCK_SIZE}-2. The internal storage is limited to 2GB
 * total byte storage.
 *
 * @lucene.internal
 */
public final class BytesRefHash implements Accountable { // 将所有的 BytesRef 对象存储到一个连续的存储空间中，并且使得能在查询阶段达到 0(1)的时间复杂度。
  private static final long BASE_RAM_BYTES =            // 类的解释: https://www.amazingkoala.com.cn/Lucene/gongjulei/2019/0218/32.html
      RamUsageEstimator.shallowSizeOfInstance(BytesRefHash.class)
          +
          // size of Counter
          RamUsageEstimator.primitiveSizes.get(long.class);

  public static final int DEFAULT_CAPACITY = 16;

  // the following fields are needed by comparator,
  // so package private to prevent access$-methods:
  final ByteBlockPool pool; // pool.bytesbuffer 存储着所有的 BytesRef
  int[] bytesStart; // 下标是 termID，数组元素是 termID 对应的 BytesRef 值在 buffers[][]中的起始位置

  private int hashSize;
  private int hashHalfSize;
  private int hashMask;
  private int count; // count + 1的值是 存储了 BytesRef的个数.    三个数组的关系: ids -> bytesStart -> pool.buffer
  private int lastCount = -1;
  private int[] ids; // 在初始化的时候,全部赋予了 -1 // 下标是 BytesRef 对利用 MurmurHash 算法计算出的 hash 值，ids[]数组元素则是 termID
  private final BytesStartArray bytesStartArray;
  private Counter bytesUsed;

  /**
   * Creates a new {@link BytesRefHash} with a {@link ByteBlockPool} using a {@link
   * DirectAllocator}.
   */
  public BytesRefHash() {
    this(new ByteBlockPool(new DirectAllocator()));
  }

  /** Creates a new {@link BytesRefHash} */
  public BytesRefHash(ByteBlockPool pool) {
    this(pool, DEFAULT_CAPACITY, new DirectBytesStartArray(DEFAULT_CAPACITY));
  }

  /** Creates a new {@link BytesRefHash} */
  public BytesRefHash(ByteBlockPool pool, int capacity, BytesStartArray bytesStartArray) {
    hashSize = capacity;
    hashHalfSize = hashSize >> 1;
    hashMask = hashSize - 1;
    this.pool = pool;
    ids = new int[hashSize];
    Arrays.fill(ids, -1);
    this.bytesStartArray = bytesStartArray;
    bytesStart = bytesStartArray.init();
    bytesUsed =
        bytesStartArray.bytesUsed() == null ? Counter.newCounter() : bytesStartArray.bytesUsed();
    bytesUsed.addAndGet(hashSize * Integer.BYTES);
  }

  /**
   * Returns the number of {@link BytesRef} values in this {@link BytesRefHash}.
   *
   * @return the number of {@link BytesRef} values in this {@link BytesRefHash}.
   */
  public int size() {
    return count;
  }

  /**
   * Populates and returns a {@link BytesRef} with the bytes for the given bytesID.
   *
   * <p>Note: the given bytesID must be a positive integer less than the current size ({@link
   * #size()})
   *
   * @param bytesID the id
   * @param ref the {@link BytesRef} to populate
   * @return the given BytesRef instance populated with the bytes for the given bytesID
   */
  public BytesRef get(int bytesID, BytesRef ref) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID < bytesStart.length : "bytesID exceeds byteStart len: " + bytesStart.length;
    pool.setBytesRef(ref, bytesStart[bytesID]);
    return ref;
  }

  /**
   * Returns the ids array in arbitrary order. Valid ids start at offset of 0 and end at a limit of
   * {@link #size()} - 1
   *
   * <p>Note: This is a destructive operation. {@link #clear()} must be called in order to reuse
   * this {@link BytesRefHash} instance.
   *
   * @lucene.internal
   */
  public int[] compact() {
    assert bytesStart != null : "bytesStart is null - not initialized";
    int upto = 0;
    for (int i = 0; i < hashSize; i++) {
      if (ids[i] != -1) {
        if (upto < i) {
          ids[upto] = ids[i];
          ids[i] = -1;
        }
        upto++;
      }
    }

    assert upto == count;
    lastCount = count;
    return ids;
  }

  /**
   * Returns the values array sorted by the referenced byte values.
   *
   * <p>Note: This is a destructive operation. {@link #clear()} must be called in order to reuse
   * this {@link BytesRefHash} instance.
   */
  public int[] sort() {
    final int[] compact = compact();
    new StringMSBRadixSorter() {

      BytesRef scratch = new BytesRef();

      @Override
      protected void swap(int i, int j) {
        int tmp = compact[i];
        compact[i] = compact[j];
        compact[j] = tmp;
      }

      @Override
      protected BytesRef get(int i) {
        pool.setBytesRef(scratch, bytesStart[compact[i]]);
        return scratch;
      }
    }.sort(0, count);
    return compact;
  }

  private boolean equals(int id, BytesRef b) {
    final int textStart = bytesStart[id];
    final byte[] bytes = pool.buffers[textStart >> BYTE_BLOCK_SHIFT];
    int pos = textStart & BYTE_BLOCK_MASK;
    final int length;
    final int offset;
    if ((bytes[pos] & 0x80) == 0) {
      // length is 1 byte
      length = bytes[pos];
      offset = pos + 1;
    } else {
      // length is 2 bytes
      length = ((short) BitUtil.VH_BE_SHORT.get(bytes, pos)) & 0x7FFF;
      offset = pos + 2;
    }
    return Arrays.equals(bytes, offset, offset + length, b.bytes, b.offset, b.offset + b.length);
  }

  private boolean shrink(int targetSize) {
    // Cannot use ArrayUtil.shrink because we require power
    // of 2:
    int newSize = hashSize;
    while (newSize >= 8 && newSize / 4 > targetSize) {
      newSize /= 2;
    }
    if (newSize != hashSize) {
      bytesUsed.addAndGet(Integer.BYTES * -(hashSize - newSize));
      hashSize = newSize;
      ids = new int[hashSize];
      Arrays.fill(ids, -1);
      hashHalfSize = newSize / 2;
      hashMask = newSize - 1;
      return true;
    } else {
      return false;
    }
  }

  /** Clears the {@link BytesRef} which maps to the given {@link BytesRef} */
  public void clear(boolean resetPool) {
    lastCount = count;
    count = 0;
    if (resetPool) {
      pool.reset(false, false); // we don't need to 0-fill the buffers
    }
    bytesStart = bytesStartArray.clear();
    if (lastCount != -1 && shrink(lastCount)) {
      // shrink clears the hash entries
      return;
    }
    Arrays.fill(ids, -1);
  }

  public void clear() {
    clear(true);
  }

  /** Closes the BytesRefHash and releases all internally used memory */
  public void close() {
    clear(true);
    ids = null;
    bytesUsed.addAndGet(Integer.BYTES * -hashSize);
  }

  /**
   * Adds a new {@link BytesRef}
   *
   * @param bytes the bytes to hash
   * @return the id the given bytes are hashed if there was no mapping for the given bytes,
   *     otherwise <code>(-(id)-1)</code>. This guarantees that the return value will always be
   *     &gt;= 0 if the given bytes haven't been hashed before.
   * @throws MaxBytesLengthExceededException if the given bytes are {@code > 2 +} {@link
   *     ByteBlockPool#BYTE_BLOCK_SIZE}
   */
  public int add(BytesRef bytes) { //如果之前没有处理过, 返回hash过的id. 如果处理过 返回1-id, 可以保证如果之前没hash过,
    assert bytesStart != null : "Bytesstart is null - not initialized"; // 返回值永远大于等于0
    final int length = bytes.length;
    // final position
    final int hashPos = findHash(bytes); // 返回murmurhash在hashsize下的slot
    int e = ids[hashPos]; // 默认-1, 说明没有这个

    if (e == -1) {
      // new entry
      final int len2 = 2 + bytes.length; // +2 是因为要留出两个byte来存储 length 本身
      if (len2 + pool.byteUpto > BYTE_BLOCK_SIZE) {  //如果写入这一行, 则超出这一行大小
        if (len2 > BYTE_BLOCK_SIZE) { // 超出了整个行大小
          throw new MaxBytesLengthExceededException(
              "bytes can be at most " + (BYTE_BLOCK_SIZE - 2) + " in length; got " + bytes.length);
        }
        pool.nextBuffer(); // 没超出一个hang大小的, 则可以新建一个buffer, 来填写 bytes
      }
      final byte[] buffer = pool.buffer; // 还没写过的,也可能是新扩出来的
      final int bufferUpto = pool.byteUpto; //
      if (count >= bytesStart.length) { // bytesStarts 整个下标数组已经跟不上容量了
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: " + bytesStart.length;
      }
      e = count++; // count + 1 的值是存储的bytesRef的个数
      // 则 e 就是下标, 也就是 termID, 也就是 ids[x]的值; 数组元素是 buffer二维数组的起始位置
      bytesStart[e] = bufferUpto + pool.byteOffset; // byteOffset 是在二维数组的每个一维的起始位置, 而bufferUpto是在此一维内的位置

      // We first encode the length, followed by the
      // bytes. Length is encoded as vInt, but will consume
      // 1 or 2 bytes at most (we reject too-long terms,
      // above).
      if (length < 128) {
        // 1 byte to store length
        buffer[bufferUpto] = (byte) length; // 第一个byte存长度, 此刻长度是小于128的, 一个byte可以存的下
        pool.byteUpto += length + 1; // 在head buffer 的位置, 如果length=127, 则此处等于128,
        assert length >= 0 : "Length must be positive: " + length;
        System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 1, length);
      } else { // 如果很大, 大于等于128个, 则用两个byte, 即short存储长度
        // 2 byte to store length
        BitUtil.VH_BE_SHORT.set(buffer, bufferUpto, (short) (length | 0x8000)); // VarHandle
        pool.byteUpto += length + 2;
        System.arraycopy(bytes.bytes, bytes.offset, buffer, bufferUpto + 2, length);
      }
      assert ids[hashPos] == -1;
      ids[hashPos] = e; // 好, 所以 e 就是termId

      if (count == hashHalfSize) {
        rehash(2 * hashSize, true);
      }
      return e;
    } // +1 是为了避免0的情况,不知道是第一次,还是不是第一次. 如果本身是0, 返回+0 -0, 而通过+1,再取负数,则可以避开0
    return -(e + 1); // 这里上下的两个return分别返回一个负数跟正数，为了区分当前处理的这个域值是不是第一次处理，是的话返回一个正数
  }

  /**
   * Returns the id of the given {@link BytesRef}.
   *
   * @param bytes the bytes to look for
   * @return the id of the given bytes, or {@code -1} if there is no mapping for the given bytes.
   */
  public int find(BytesRef bytes) {
    return ids[findHash(bytes)];
  }

  private int findHash(BytesRef bytes) {
    assert bytesStart != null : "bytesStart is null - not initialized";

    int code = doHash(bytes.bytes, bytes.offset, bytes.length);

    // final position
    int hashPos = code & hashMask;
    int e = ids[hashPos];
    if (e != -1 && !equals(e, bytes)) {
      // Conflict; use linear probe to find an open slot
      // (see LUCENE-5604):
      do {
        code++;
        hashPos = code & hashMask;
        e = ids[hashPos];
      } while (e != -1 && !equals(e, bytes));
    }

    return hashPos;
  }

  /**
   * Adds a "arbitrary" int offset instead of a BytesRef term. This is used in the indexer to hold
   * the hash for term vectors, because they do not redundantly store the byte[] term directly and
   * instead reference the byte[] term already stored by the postings BytesRefHash. See add(int
   * textStart) in TermsHashPerField.
   */
  public int addByPoolOffset(int offset) {
    assert bytesStart != null : "Bytesstart is null - not initialized";
    // final position
    int code = offset;
    int hashPos = offset & hashMask;
    int e = ids[hashPos];
    if (e != -1 && bytesStart[e] != offset) {
      // Conflict; use linear probe to find an open slot
      // (see LUCENE-5604):
      do {
        code++;
        hashPos = code & hashMask;
        e = ids[hashPos];
      } while (e != -1 && bytesStart[e] != offset);
    }
    if (e == -1) {
      // new entry
      if (count >= bytesStart.length) {
        bytesStart = bytesStartArray.grow();
        assert count < bytesStart.length + 1 : "count: " + count + " len: " + bytesStart.length;
      }
      e = count++;
      bytesStart[e] = offset;
      assert ids[hashPos] == -1;
      ids[hashPos] = e;

      if (count == hashHalfSize) {
        rehash(2 * hashSize, false);
      }
      return e;
    }
    return -(e + 1);
  }

  /**
   * Called when hash is too small ({@code > 50%} occupied) or too large ({@code < 20%} occupied).
   */
  private void rehash(final int newSize, boolean hashOnData) {
    final int newMask = newSize - 1;
    bytesUsed.addAndGet(Integer.BYTES * (newSize));
    final int[] newHash = new int[newSize];
    Arrays.fill(newHash, -1);
    for (int i = 0; i < hashSize; i++) {
      final int e0 = ids[i];
      if (e0 != -1) {
        int code;
        if (hashOnData) {
          final int off = bytesStart[e0];
          final int start = off & BYTE_BLOCK_MASK;
          final byte[] bytes = pool.buffers[off >> BYTE_BLOCK_SHIFT];
          final int len;
          int pos;
          if ((bytes[start] & 0x80) == 0) {
            // length is 1 byte
            len = bytes[start];
            pos = start + 1;
          } else {
            len = ((short) BitUtil.VH_BE_SHORT.get(bytes, start)) & 0x7FFF;
            pos = start + 2;
          }
          code = doHash(bytes, pos, len);
        } else {
          code = bytesStart[e0];
        }

        int hashPos = code & newMask;
        assert hashPos >= 0;
        if (newHash[hashPos] != -1) {
          // Conflict; use linear probe to find an open slot
          // (see LUCENE-5604):
          do {
            code++;
            hashPos = code & newMask;
          } while (newHash[hashPos] != -1);
        }
        newHash[hashPos] = e0;
      }
    }

    hashMask = newMask;
    bytesUsed.addAndGet(Integer.BYTES * (-ids.length));
    ids = newHash;
    hashSize = newSize;
    hashHalfSize = newSize / 2;
  }

  // TODO: maybe use long?  But our keys are typically short...
  private int doHash(byte[] bytes, int offset, int length) {
    return StringHelper.murmurhash3_x86_32(bytes, offset, length, StringHelper.GOOD_FAST_HASH_SEED);
  }

  /**
   * reinitializes the {@link BytesRefHash} after a previous {@link #clear()} call. If {@link
   * #clear()} has not been called previously this method has no effect.
   */
  public void reinit() {
    if (bytesStart == null) {
      bytesStart = bytesStartArray.init();
    }

    if (ids == null) {
      ids = new int[hashSize];
      bytesUsed.addAndGet(Integer.BYTES * hashSize);
    }
  }

  /**
   * Returns the bytesStart offset into the internally used {@link ByteBlockPool} for the given
   * bytesID
   *
   * @param bytesID the id to look up
   * @return the bytesStart offset into the internally used {@link ByteBlockPool} for the given id
   */
  public int byteStart(int bytesID) {
    assert bytesStart != null : "bytesStart is null - not initialized";
    assert bytesID >= 0 && bytesID < count : bytesID;
    return bytesStart[bytesID];
  }

  @Override
  public long ramBytesUsed() {
    long size =
        BASE_RAM_BYTES
            + RamUsageEstimator.sizeOfObject(bytesStart)
            + RamUsageEstimator.sizeOfObject(ids)
            + RamUsageEstimator.sizeOfObject(pool);
    return size;
  }

  /**
   * Thrown if a {@link BytesRef} exceeds the {@link BytesRefHash} limit of {@link
   * ByteBlockPool#BYTE_BLOCK_SIZE}-2.
   */
  @SuppressWarnings("serial")
  public static class MaxBytesLengthExceededException extends RuntimeException {
    MaxBytesLengthExceededException(String message) {
      super(message);
    }
  }

  /** Manages allocation of the per-term addresses. */
  public abstract static class BytesStartArray {
    /**
     * Initializes the BytesStartArray. This call will allocate memory
     *
     * @return the initialized bytes start array
     */
    public abstract int[] init();

    /**
     * Grows the {@link BytesStartArray}
     *
     * @return the grown array
     */
    public abstract int[] grow();

    /**
     * clears the {@link BytesStartArray} and returns the cleared instance.
     *
     * @return the cleared instance, this might be <code>null</code>
     */
    public abstract int[] clear();

    /**
     * A {@link Counter} reference holding the number of bytes used by this {@link BytesStartArray}.
     * The {@link BytesRefHash} uses this reference to track it memory usage
     *
     * @return a {@link AtomicLong} reference holding the number of bytes used by this {@link
     *     BytesStartArray}.
     */
    public abstract Counter bytesUsed();
  }

  /**
   * A simple {@link BytesStartArray} that tracks memory allocation using a private {@link Counter}
   * instance.
   */
  public static class DirectBytesStartArray extends BytesStartArray {
    // TODO: can't we just merge this w/
    // TrackingDirectBytesStartArray...?  Just add a ctor
    // that makes a private bytesUsed?

    protected final int initSize;
    private int[] bytesStart;
    private final Counter bytesUsed;

    public DirectBytesStartArray(int initSize, Counter counter) {
      this.bytesUsed = counter;
      this.initSize = initSize;
    }

    public DirectBytesStartArray(int initSize) {
      this(initSize, Counter.newCounter());
    }

    @Override
    public int[] clear() {
      return bytesStart = null;
    }

    @Override
    public int[] grow() {
      assert bytesStart != null;
      return bytesStart = ArrayUtil.grow(bytesStart, bytesStart.length + 1);
    }

    @Override
    public int[] init() {
      return bytesStart = new int[ArrayUtil.oversize(initSize, Integer.BYTES)];
    }

    @Override
    public Counter bytesUsed() {
      return bytesUsed;
    }
  }
}
