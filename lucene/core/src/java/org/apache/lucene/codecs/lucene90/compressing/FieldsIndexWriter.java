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
package org.apache.lucene.codecs.lucene90.compressing;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.ChecksumIndexInput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.packed.DirectMonotonicWriter;

/**
 * Efficient index format for block-based {@link Codec}s.
 *
 * <p>For each block of compressed stored fields, this stores the first document of the block and
 * the start pointer of the block in a {@link DirectMonotonicWriter}. At read time, the docID is
 * binary-searched in the {@link DirectMonotonicReader} that records doc IDS, and the returned index
 * is used to look up the start pointer in the {@link DirectMonotonicReader} that records start
 * pointers.
 *
 * @lucene.internal
 */ // 按块的方式存储field的各类正排信息: storedfield termvector
public final class FieldsIndexWriter implements Closeable {

  static final int VERSION_START = 0;
  static final int VERSION_CURRENT = 0;
  // dir name suffix extension codecName 都是用来创 真正的 索引文件的
  private final Directory dir;
  private final String name;
  private final String suffix;
  private final String extension;
  private final String codecName;
  private final byte[] id;
  private final int blockShift; // MonotonicDirectWriter所需的参数
  private final IOContext ioContext;
  private IndexOutput docsOut; // 临时文件, 写文档的个数
  private IndexOutput filePointersOut; // 临时文件, 用来写个trunk在真正数据文件中的起始位置. 长度
  private int totalDocs;
  private int totalChunks;
  private long previousFP; // 初始化 默认0

  FieldsIndexWriter(
      Directory dir,
      String name,
      String suffix,
      String extension,
      String codecName,
      byte[] id,
      int blockShift,
      IOContext ioContext)
      throws IOException {
    this.dir = dir;
    this.name = name;
    this.suffix = suffix;
    this.extension = extension; // 拓展文件 tvx 或者 fdx
    this.codecName = codecName;
    this.id = id;
    this.blockShift = blockShift;
    this.ioContext = ioContext;
    this.docsOut = dir.createTempOutput(name, codecName + "-doc_ids", ioContext);
    boolean success = false;
    try {
      CodecUtil.writeHeader(docsOut, codecName + "Docs", VERSION_CURRENT);
      filePointersOut = dir.createTempOutput(name, codecName + "file_pointers", ioContext);
      CodecUtil.writeHeader(filePointersOut, codecName + "FilePointers", VERSION_CURRENT);
      success = true;
    } finally {
      if (success == false) {
        close();
      }
    }
  }
  // 添加一个新的index，index就是用来定位doc属于哪个chunk，以及chunk在数据文件中的起始位置。numDocs是chunk中的文档总数，后面真正序列化到正式的索引文件会通过换算，得到的是每个chunk的起始docID。
  void writeIndex(int numDocs, long startPointer) throws IOException {
    assert startPointer >= previousFP;
    docsOut.writeVInt(numDocs); // 向 临时的docsOut，标识一个block写入多少个文档
    filePointersOut.writeVLong(startPointer - previousFP); // 在临时文件中写入fdt的file pointers,写入的是长度?
    previousFP = startPointer;  // 是上一个 chunk的在真正的data里的长度?如果是stored 则是fdt,如果是termvector,则是 tvd
    totalDocs += numDocs;       // 为什么是上一个？首先是因为这个chunk在这个流中的长度是不确定的，因为要经过运行时选中的“压缩策略”，以及
    totalChunks++;              //  第一个长度,应该是fieldsStream(fdt,tvd)的HEADER长度
  }
  // metaOut是元信息索引文件fdm, tvm.  maxPointer是fdt,tvd的pointer.  numDocs是目前处理过的doc的count
  void finish(int numDocs, long maxPointer, IndexOutput metaOut) throws IOException {
    if (numDocs != totalDocs) {
      throw new IllegalStateException("Expected " + numDocs + " docs, but got " + totalDocs);
    }
    CodecUtil.writeFooter(docsOut);
    CodecUtil.writeFooter(filePointersOut);
    IOUtils.close(docsOut, filePointersOut); // 完成对两个 临时 文件的写入

    try (IndexOutput dataOut = // 新建一个output, 构建的是 fdx tvx, 即chunk的索引
        dir.createOutput(IndexFileNames.segmentFileName(name, suffix, extension), ioContext)) {
      CodecUtil.writeIndexHeader(dataOut, codecName + "Idx", VERSION_CURRENT, id, suffix);
      // 从这里可以看出 meta：fdm tvm的文件构造了： header ， numdocs, blockshift , totalChunks+1
      metaOut.writeInt(numDocs);
      metaOut.writeInt(blockShift);
      metaOut.writeInt(totalChunks + 1);
      metaOut.writeLong(dataOut.getFilePointer()); // fdm, tvm 写入fdx tvx的当前的pointer
      // 第一个临时文件， docsOut ，里面记录着每个chunk内的doc的数量  
      try (ChecksumIndexInput docsIn =
          dir.openChecksumInput(docsOut.getName(), IOContext.READONCE)) {
        CodecUtil.checkHeader(docsIn, codecName + "Docs", VERSION_CURRENT, VERSION_CURRENT);
        Throwable priorE = null;
        try {
          final DirectMonotonicWriter docs =
              DirectMonotonicWriter.getInstance(metaOut, dataOut, totalChunks + 1, blockShift);
          long doc = 0;
          docs.add(doc); // doc, 即docId, 先加入0, 0是第一个chunk的第一个doc
          for (int i = 0; i < totalChunks; ++i) {
            doc += docsIn.readVInt(); // 读取来的是每个chunk里存着的docId的数量, 但它就是下一个chunk的docId的id
            docs.add(doc); // monotonic 里存的是每个chunk的第一个doc的docId
          } // 所以docs里存的是 [chunk0_firstDocId, chunk1_firstDocId......]
          docs.finish();
          if (doc != totalDocs) {
            throw new CorruptIndexException("Docs don't add up", docsIn);
          }
        } catch (Throwable e) {
          priorE = e;
        } finally {
          CodecUtil.checkFooter(docsIn, priorE);
        }
      }
      dir.deleteFile(docsOut.getName()); // 删除临时文件
      docsOut = null;
      // 每个chunk的开始位置和上一个chunk的开始位置的差值，约等于记录了每个chunk在文件中的长度, 写在了 filePointersOut临时文件里, 现在要转义到某个meta stream里了(fdm tvm)
      metaOut.writeLong(dataOut.getFilePointer());
      try (ChecksumIndexInput filePointersIn =
          dir.openChecksumInput(filePointersOut.getName(), IOContext.READONCE)) {
        CodecUtil.checkHeader(
            filePointersIn, codecName + "FilePointers", VERSION_CURRENT, VERSION_CURRENT);
        Throwable priorE = null;
        try {
          final DirectMonotonicWriter filePointers =
              DirectMonotonicWriter.getInstance(metaOut, dataOut, totalChunks + 1, blockShift);
          long fp = 0;
          for (int i = 0; i < totalChunks; ++i) {
            fp += filePointersIn.readVLong(); // fp = 0, 加上长度, 等于地址. 第一个readVlong,得到的是HEADER的长度, 所以, 其实0+HEADER,
            filePointers.add(fp);    // 等于的就是的第一个chunk的起始地址, 然后i=1的时候,readVlong得到的是第一个chunk的长度,加上fp,其实是第二个chunk的起始地址
          }
          if (maxPointer < fp) {
            throw new CorruptIndexException("File pointers don't add up", filePointersIn);
          }
          filePointers.add(maxPointer); // 最终加上maxPointer, 是data文件fdt, tvd的最终末尾地址.需要这个地址是因为, 读取最后一个chunk的时候,需要知道
          filePointers.finish();        // 读的哪里停止,  start_of_final_chunk ----- maxPointer 中间的就是最后一个chunk
        } catch (Throwable e) {
          priorE = e;
        } finally {
          CodecUtil.checkFooter(filePointersIn, priorE);
        }
      }
      dir.deleteFile(filePointersOut.getName());
      filePointersOut = null;

      metaOut.writeLong(dataOut.getFilePointer()); // metaOut 写入fdm,tvm的最后pointer
      metaOut.writeLong(maxPointer); // 以及fdt,tvd的最后数据地址(再往后就是 rooter 了) // 在上一层的调用处写rooter

      CodecUtil.writeFooter(dataOut); //
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(docsOut, filePointersOut);
    } finally {
      List<String> fileNames = new ArrayList<>();
      if (docsOut != null) {
        fileNames.add(docsOut.getName());
      }
      if (filePointersOut != null) {
        fileNames.add(filePointersOut.getName());
      }
      try {
        IOUtils.deleteFiles(dir, fileNames);
      } finally {
        docsOut = filePointersOut = null;
      }
    }
  }
}
