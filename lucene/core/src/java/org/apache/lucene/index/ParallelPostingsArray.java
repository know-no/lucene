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

import org.apache.lucene.util.ArrayUtil;
// 倒排表 termId -> 信息
class ParallelPostingsArray { // 这就是 倒排表 类; 它的两个子类, 多了几个属性: 这几个多了的属性会随着每次处理完term,doc而变化
  static final int BYTES_PER_POSTING = 3 * Integer.BYTES;

  final int size;         // 这三个属性对于字段中的同一个term是不变的
  final int[] textStarts; // maps term ID to the terms's text start in the bytesHash // term的值本身在bytesHash中的开始位置
  final int[] addressOffset; // maps term ID to current stream address//值是termID对应stream记录在IntBlockPool中当前写入位置的offset
  final int[] byteStarts; // maps term ID to stream start offset in the byte pool//值是该term的stream在ByteBlockPool中的下一个可以写入的位置

  ParallelPostingsArray(final int size) {
    this.size = size;
    textStarts = new int[size];
    addressOffset = new int[size];
    byteStarts = new int[size];
  }

  int bytesPerPosting() {
    return BYTES_PER_POSTING;
  }

  ParallelPostingsArray newInstance(int size) {
    return new ParallelPostingsArray(size);
  }

  final ParallelPostingsArray grow() { // 扩容,毕竟初始的size 只有2
    int newSize = ArrayUtil.oversize(size + 1, bytesPerPosting());
    ParallelPostingsArray newArray = newInstance(newSize);
    copyTo(newArray, size);
    return newArray;
  }

  void copyTo(ParallelPostingsArray toArray, int numToCopy) {
    System.arraycopy(textStarts, 0, toArray.textStarts, 0, numToCopy);
    System.arraycopy(addressOffset, 0, toArray.addressOffset, 0, numToCopy);
    System.arraycopy(byteStarts, 0, toArray.byteStarts, 0, numToCopy);
  }
}
