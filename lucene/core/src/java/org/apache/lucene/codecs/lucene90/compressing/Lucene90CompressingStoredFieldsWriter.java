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

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.StoredFieldsReader;
import org.apache.lucene.codecs.StoredFieldsWriter;
import org.apache.lucene.codecs.compressing.CompressionMode;
import org.apache.lucene.codecs.compressing.Compressor;
import org.apache.lucene.codecs.compressing.MatchingReaders;
import org.apache.lucene.codecs.lucene90.compressing.Lucene90CompressingStoredFieldsReader.SerializedDocument;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.DataOutput;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.BitUtil;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.packed.PackedInts;

/**
 * {@link StoredFieldsWriter} impl for {@link Lucene90CompressingStoredFieldsFormat}.
 *
 * @lucene.experimental
 */
public final class Lucene90CompressingStoredFieldsWriter extends StoredFieldsWriter {

  /** Extension of stored fields file */
  public static final String FIELDS_EXTENSION = "fdt";
  /** Extension of stored fields index */
  public static final String INDEX_EXTENSION = "fdx";
  /** Extension of stored fields meta */
  public static final String META_EXTENSION = "fdm";
  /** Codec name for the index. */
  public static final String INDEX_CODEC_NAME = "Lucene90FieldsIndex";

  static final int STRING = 0x00;
  static final int BYTE_ARR = 0x01;
  static final int NUMERIC_INT = 0x02;
  static final int NUMERIC_FLOAT = 0x03;
  static final int NUMERIC_LONG = 0x04;
  static final int NUMERIC_DOUBLE = 0x05;

  static final int TYPE_BITS = PackedInts.bitsRequired(NUMERIC_DOUBLE);
  static final int TYPE_MASK = (int) PackedInts.maxValue(TYPE_BITS);

  static final int VERSION_START = 1;
  static final int VERSION_CURRENT = VERSION_START;
  static final int META_VERSION_START = 0;

  private final String segment;         // indexWriter 写 fdx, fdt, fdm的前前缀
  private FieldsIndexWriter indexWriter; //索引生成工具:用来生成所有chunk中的起始doc编号和chunk在数据文件中的位置信息，方便读取的时候快速定位doc所属的chunk，并从文件中读取chunk。总体逻辑是先使用临时文件存储前面说的两个信息，在真正生成索引文件的时候，使用DirectMonotonicWriter进行压缩存储，减小索引文件大小。
  private IndexOutput metaStream, fieldsStream; // 这liang个类分别是写：fdm，fdt的
  // fdm  field data meta,  fdt 存的正派数据
  private Compressor compressor;
  private final CompressionMode compressionMode;
  private final int chunkSize;
  private final int maxDocsPerChunk;

  private final ByteBuffersDataOutput bufferedDocs; // 缓存所有的field的值;要写入一个chunk的
  private int[] numStoredFields; // number of stored fields // 下标是当前chunk中的docID的偏移量，值是对应doc的字段个数
  private int[] endOffsets; // end offsets in bufferedDocs // 下标是当前chunk中的docID的偏移量，值是对应doc的所有需要store的数据在bufferedDocs中的结束位置
  private int docBase; // doc ID at the beginning of the chunk // chunk中的起始id
  private int numBufferedDocs; // docBase + numBufferedDocs == current doc ID // chunk中的doc个数,也可以理解为在这个chunk中的id

  private long numChunks;
  private long numDirtyChunks; // number of incomplete compressed blocks written // dirtyChunk总数，未满足生成chunk的条件时，强制生成的chunk是dirtyChunk
  private long numDirtyDocs; // cumulative number of missing docs in incomplete chunks // dirtyDoc总数，dirtyChunk中的doc是dirtyDoc

  /** Sole constructor. */
  Lucene90CompressingStoredFieldsWriter(
      Directory directory,
      SegmentInfo si,
      String segmentSuffix,
      IOContext context,
      String formatName,
      CompressionMode compressionMode,
      int chunkSize,
      int maxDocsPerChunk,
      int blockShift)
      throws IOException {
    assert directory != null;
    this.segment = si.name;
    this.compressionMode = compressionMode;
    this.compressor = compressionMode.newCompressor();
    this.chunkSize = chunkSize;
    this.maxDocsPerChunk = maxDocsPerChunk;
    this.docBase = 0;
    this.bufferedDocs = ByteBuffersDataOutput.newResettableInstance();
    this.numStoredFields = new int[16];
    this.endOffsets = new int[16];
    this.numBufferedDocs = 0;

    boolean success = false;
    try {
      metaStream =
          directory.createOutput(
              IndexFileNames.segmentFileName(segment, segmentSuffix, META_EXTENSION), context);
      CodecUtil.writeIndexHeader(
          metaStream, INDEX_CODEC_NAME + "Meta", VERSION_CURRENT, si.getId(), segmentSuffix);
      assert CodecUtil.indexHeaderLength(INDEX_CODEC_NAME + "Meta", segmentSuffix)
          == metaStream.getFilePointer();

      fieldsStream =
          directory.createOutput(
              IndexFileNames.segmentFileName(segment, segmentSuffix, FIELDS_EXTENSION), context);
      CodecUtil.writeIndexHeader(
          fieldsStream, formatName, VERSION_CURRENT, si.getId(), segmentSuffix);
      assert CodecUtil.indexHeaderLength(formatName, segmentSuffix)
          == fieldsStream.getFilePointer();

      indexWriter =
          new FieldsIndexWriter(
              directory,
              segment,
              segmentSuffix,
              INDEX_EXTENSION,
              INDEX_CODEC_NAME,
              si.getId(),
              blockShift,
              context);

      metaStream.writeVInt(chunkSize); // 嘿嘿, 第一步就是写入了chunkSize

      success = true;
    } finally {
      if (!success) {
        IOUtils.closeWhileHandlingException(metaStream, fieldsStream, indexWriter);
      }
    }
  }

  @Override
  public void close() throws IOException {
    try {
      IOUtils.close(metaStream, fieldsStream, indexWriter, compressor);
    } finally {
      metaStream = null;
      fieldsStream = null;
      indexWriter = null;
      compressor = null;
    }
  }

  private int numStoredFieldsInDoc; // 处理一个doc时， 已经处理的field的个数

  @Override
  public void startDocument() throws IOException {}

  @Override // 结束对一个doc的处理
  public void finishDocument() throws IOException { // 结束一个doc的处理， 判断本批缓存里的doc数据是否到达限制了
    if (numBufferedDocs == this.numStoredFields.length) { // 判断 numStoredFields数组容量是否用完了，用完了就扩容
      final int newLength = ArrayUtil.oversize(numBufferedDocs + 1, 4);
      this.numStoredFields = ArrayUtil.growExact(this.numStoredFields, newLength);
      endOffsets = ArrayUtil.growExact(endOffsets, newLength);
    }
    this.numStoredFields[numBufferedDocs] = numStoredFieldsInDoc;//此doc的处理了的storedfield的数目，写入数组
    numStoredFieldsInDoc = 0;
    endOffsets[numBufferedDocs] = Math.toIntExact(bufferedDocs.size()); // 记录当前doc的storedField在缓存output中的结束的位置
    ++numBufferedDocs;
    if (triggerFlush()) { // 判断是否triger flush : 1. 大小超过一个chunk 2. doc个数超出一个chunk的限制
      flush(false); // 本质上是想要生成一个chunk, 如果处理的doc的数量,或者存储大小符合阈值了 // 到此为止, 没有任何和 FieldsIndexWriter的关系
    }
  }

  private static void saveInts(int[] values, int length, DataOutput out) throws IOException {
    if (length == 1) { // 极致压缩， 如果只有一个， 直接写int， 如果是多个
      out.writeVInt(values[0]);
    } else {
      StoredFieldsInts.writeInts(values, 0, length, out);
    }
  }

  private void writeHeader( // 写chunk的header, 即不写入chunk的本身的数据, 而是写本身数据的一些meta信息, 并且也是写入了fieldsStrem(fdt,tvd)
      int docBase,//docBase
      int numBufferedDocs,//this.numBufferedDocs//docBase + numBufferedDocs == current docID//chunk中的doc个数也可以理解为在这个chunk中的id
      int[] numStoredFields,//this.numStoredFields //下标是当前chunk中的docID的偏移量，值是对应doc的字段个数
      int[] lengths, // 每个doc的所有storedfield 在 buffer 里的长度
      boolean sliced,
      boolean dirtyChunk)
      throws IOException {
    final int slicedBit = sliced ? 1 : 0;
    final int dirtyBit = dirtyChunk ? 2 : 0;
    // save docBase and numBufferedDocs
    fieldsStream.writeVInt(docBase); // 正排数据， 写入dockbase
    fieldsStream.writeVInt((numBufferedDocs << 2) | dirtyBit | slicedBit);
    // docBase  （numBufferedDocs|dirtyBit|slicedBit)）
    // save numStoredFields
    saveInts(numStoredFields, numBufferedDocs, fieldsStream);// 存每个doc拥有的storedfield的数量

    // save lengths
    saveInts(lengths, numBufferedDocs, fieldsStream); // 还会写入length, 即end位置在buffer中的位置
  }

  private boolean triggerFlush() {
    return bufferedDocs.size() >= chunkSize
        || // chunks of at least chunkSize bytes
        numBufferedDocs >= maxDocsPerChunk;
  }

  private void flush(boolean force) throws IOException { // 刷写出一个chunk来
    assert triggerFlush() != force;
    numChunks++; // 全局变量，标识这个writer会生成的 chunk的数量
    if (force) { // force, 代表着可能是要关闭了，或者强制flush, 反正chunk的大小还没达到限制上限
      numDirtyChunks++; // incomplete: we had to force this flush
      numDirtyDocs += numBufferedDocs;
    }         // 这里的index不是lucene的index概念， 而是通常意义上的索引;向临时meta里写入,这次要flush的chunk的文档的个数
    indexWriter.writeIndex(numBufferedDocs, fieldsStream.getFilePointer());//并且把上一个trunk在data里的长度写入临时文件 idxWriter的flush不再这里
    // 将fdt，即chunk形式存储的store的字段的trunk的位置信息，写入到 indexWriter里(具体是临时文件)
    // transform end offsets into lengths
    final int[] lengths = endOffsets;
    for (int i = numBufferedDocs - 1; i > 0; --i) {
      lengths[i] = endOffsets[i] - endOffsets[i - 1]; // endOffsets是数组，下标是docid**偏移量**，值是doc的所有storedfield是值在buffer中的end位置
      assert lengths[i] >= 0;  // 这里其实是进行了一次压缩， 可以得到doc的storedfield在buffer中的数据长度; 因为上文是偏移量，所以最小是1
    }                         // 所谓偏移， 是相对于chunk的docBase而言
    final boolean sliced = bufferedDocs.size() >= 2 * chunkSize; // 实际大小，大于了2x约束
    final boolean dirtyChunk = force;
    writeHeader(docBase, numBufferedDocs, numStoredFields, lengths, sliced, dirtyChunk);//chunk已经在内存在构建完成,写入到fieldsStream里
    // 写下header，读取的时候依据header读取,
    // compress stored fields to fieldsStream.
    //
    // TODO: do we need to slice it since we already have the slices in the buffer? Perhaps
    // we should use max-block-bits restriction on the buffer itself, then we won't have to check it
    // here.
    byte[] content = bufferedDocs.toArrayCopy(); // trunk的数据本身
    bufferedDocs.reset(); // 重置，释放空间，让gc有机会快点执行

    if (sliced) {
      // big chunk, slice it // 切片压缩
      for (int compressed = 0; compressed < content.length; compressed += chunkSize) {
        compressor.compress( // 怎么压缩的就先不管了
            content, compressed, Math.min(chunkSize, content.length - compressed), fieldsStream);
      }
    } else {
      compressor.compress(content, 0, content.length, fieldsStream);
    }

    // reset
    docBase += numBufferedDocs; // 更新 docBase
    numBufferedDocs = 0;        // 清空docId的偏移
    bufferedDocs.reset();       // 清空buffer
  }

  @Override // 在StoredFieldsConsumer中使用  // writeField只是把内容写到了自身的缓存里 bufferedDocs, 并且更新numStoredFieldsInDoc
  public void writeField(FieldInfo info, IndexableField field) throws IOException { // 写入的是field, 可以是不同的field
                                                                // 在314行的 infoAndBits, 就会记录处理的是哪个field
    ++numStoredFieldsInDoc; // 处理这个doc， 又要处理一个field了

    int bits = 0; // 这个field的数据类型的编码
    final BytesRef bytes;
    final String string;

    Number number = field.numericValue(); // 看看是否有数值类型
    if (number != null) {
      if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
        bits = NUMERIC_INT;
      } else if (number instanceof Long) {
        bits = NUMERIC_LONG;
      } else if (number instanceof Float) {
        bits = NUMERIC_FLOAT;
      } else if (number instanceof Double) {
        bits = NUMERIC_DOUBLE;
      } else {
        throw new IllegalArgumentException("cannot store numeric type " + number.getClass());
      }
      string = null;
      bytes = null;
    } else {
      bytes = field.binaryValue();
      if (bytes != null) {
        bits = BYTE_ARR;
        string = null;
      } else {
        bits = STRING;
        string = field.stringValue();
        if (string == null) {
          throw new IllegalArgumentException(
              "field "
                  + field.name()
                  + " is stored but does not have binaryValue, stringValue nor numericValue");
        }
      }
    }
    // info.number标识 field的内部序号。segment内部的. infoAndBites 是将这两者合并，合并写到底层
    final long infoAndBits = (((long) info.number) << TYPE_BITS) | bits;
    bufferedDocs.writeVLong(infoAndBits);
    // output 数据， bufferedDocs本质是 DataOutput，只不过是加了缓存的，等待其他调用来flush
    if (bytes != null) { // 写入缓存：数据的长度， 数据本身
      bufferedDocs.writeVInt(bytes.length);
      bufferedDocs.writeBytes(bytes.bytes, bytes.offset, bytes.length);
    } else if (string != null) {
      bufferedDocs.writeString(string);
    } else {
      if (number instanceof Byte || number instanceof Short || number instanceof Integer) {
        bufferedDocs.writeZInt(number.intValue());
      } else if (number instanceof Long) {
        writeTLong(bufferedDocs, number.longValue());
      } else if (number instanceof Float) {
        writeZFloat(bufferedDocs, number.floatValue());
      } else if (number instanceof Double) {
        writeZDouble(bufferedDocs, number.doubleValue());
      } else {
        throw new AssertionError("Cannot get here");
      }
    }
  }

  // -0 isn't compressed.
  static final int NEGATIVE_ZERO_FLOAT = Float.floatToIntBits(-0f);
  static final long NEGATIVE_ZERO_DOUBLE = Double.doubleToLongBits(-0d);

  // for compression of timestamps
  static final long SECOND = 1000L;
  static final long HOUR = 60 * 60 * SECOND;
  static final long DAY = 24 * HOUR;
  static final int SECOND_ENCODING = 0x40;
  static final int HOUR_ENCODING = 0x80;
  static final int DAY_ENCODING = 0xC0;

  /**
   * Writes a float in a variable-length format. Writes between one and five bytes. Small integral
   * values typically take fewer bytes.
   *
   * <p>ZFloat --&gt; Header, Bytes*?
   *
   * <ul>
   *   <li>Header --&gt; {@link DataOutput#writeByte Uint8}. When it is equal to 0xFF then the value
   *       is negative and stored in the next 4 bytes. Otherwise if the first bit is set then the
   *       other bits in the header encode the value plus one and no other bytes are read.
   *       Otherwise, the value is a positive float value whose first byte is the header, and 3
   *       bytes need to be read to complete it.
   *   <li>Bytes --&gt; Potential additional bytes to read depending on the header.
   * </ul>
   */
  static void writeZFloat(DataOutput out, float f) throws IOException {
    int intVal = (int) f;
    final int floatBits = Float.floatToIntBits(f);

    if (f == intVal && intVal >= -1 && intVal <= 0x7D && floatBits != NEGATIVE_ZERO_FLOAT) {
      // small integer value [-1..125]: single byte
      out.writeByte((byte) (0x80 | (1 + intVal)));
    } else if ((floatBits >>> 31) == 0) {
      // other positive floats: 4 bytes
      out.writeByte((byte) (floatBits >> 24));
      out.writeShort((short) (floatBits >>> 8));
      out.writeByte((byte) floatBits);
    } else {
      // other negative float: 5 bytes
      out.writeByte((byte) 0xFF);
      out.writeInt(floatBits);
    }
  }

  /**
   * Writes a float in a variable-length format. Writes between one and five bytes. Small integral
   * values typically take fewer bytes.
   *
   * <p>ZFloat --&gt; Header, Bytes*?
   *
   * <ul>
   *   <li>Header --&gt; {@link DataOutput#writeByte Uint8}. When it is equal to 0xFF then the value
   *       is negative and stored in the next 8 bytes. When it is equal to 0xFE then the value is
   *       stored as a float in the next 4 bytes. Otherwise if the first bit is set then the other
   *       bits in the header encode the value plus one and no other bytes are read. Otherwise, the
   *       value is a positive float value whose first byte is the header, and 7 bytes need to be
   *       read to complete it.
   *   <li>Bytes --&gt; Potential additional bytes to read depending on the header.
   * </ul>
   */
  static void writeZDouble(DataOutput out, double d) throws IOException {
    int intVal = (int) d;
    final long doubleBits = Double.doubleToLongBits(d);

    if (d == intVal && intVal >= -1 && intVal <= 0x7C && doubleBits != NEGATIVE_ZERO_DOUBLE) {
      // small integer value [-1..124]: single byte
      out.writeByte((byte) (0x80 | (intVal + 1)));
      return;
    } else if (d == (float) d) {
      // d has an accurate float representation: 5 bytes
      out.writeByte((byte) 0xFE);
      out.writeInt(Float.floatToIntBits((float) d));
    } else if ((doubleBits >>> 63) == 0) {
      // other positive doubles: 8 bytes
      out.writeByte((byte) (doubleBits >> 56));
      out.writeInt((int) (doubleBits >>> 24));
      out.writeShort((short) (doubleBits >>> 8));
      out.writeByte((byte) (doubleBits));
    } else {
      // other negative doubles: 9 bytes
      out.writeByte((byte) 0xFF);
      out.writeLong(doubleBits);
    }
  }

  /**
   * Writes a long in a variable-length format. Writes between one and ten bytes. Small values or
   * values representing timestamps with day, hour or second precision typically require fewer
   * bytes.
   *
   * <p>ZLong --&gt; Header, Bytes*?
   *
   * <ul>
   *   <li>Header --&gt; The first two bits indicate the compression scheme:
   *       <ul>
   *         <li>00 - uncompressed
   *         <li>01 - multiple of 1000 (second)
   *         <li>10 - multiple of 3600000 (hour)
   *         <li>11 - multiple of 86400000 (day)
   *       </ul>
   *       Then the next bit is a continuation bit, indicating whether more bytes need to be read,
   *       and the last 5 bits are the lower bits of the encoded value. In order to reconstruct the
   *       value, you need to combine the 5 lower bits of the header with a vLong in the next bytes
   *       (if the continuation bit is set to 1). Then {@link BitUtil#zigZagDecode(int)
   *       zigzag-decode} it and finally multiply by the multiple corresponding to the compression
   *       scheme.
   *   <li>Bytes --&gt; Potential additional bytes to read depending on the header.
   * </ul>
   */
  // T for "timestamp"
  static void writeTLong(DataOutput out, long l) throws IOException {
    int header;
    if (l % SECOND != 0) {
      header = 0;
    } else if (l % DAY == 0) {
      // timestamp with day precision
      header = DAY_ENCODING;
      l /= DAY;
    } else if (l % HOUR == 0) {
      // timestamp with hour precision, or day precision with a timezone
      header = HOUR_ENCODING;
      l /= HOUR;
    } else {
      // timestamp with second precision
      header = SECOND_ENCODING;
      l /= SECOND;
    }

    final long zigZagL = BitUtil.zigZagEncode(l);
    header |= (zigZagL & 0x1F); // last 5 bits
    final long upperBits = zigZagL >>> 5;
    if (upperBits != 0) {
      header |= 0x20;
    }
    out.writeByte((byte) header);
    if (upperBits != 0) {
      out.writeVLong(upperBits);
    }
  }

  @Override
  public void finish(int numDocs) throws IOException {
    if (numBufferedDocs > 0) {
      flush(true); // 先将这个类的缓存 写入 到 IndexWriter, 让它帮忙压缩下, 然后调用indexWriter.flush, 促使indexWriter将自己的压缩数据写到真实文件中
    } else {
      assert bufferedDocs.size() == 0;
    }
    if (docBase != numDocs) {
      throw new RuntimeException(
          "Wrote " + docBase + " docs, finish called with numDocs=" + numDocs);
    }
    indexWriter.finish(numDocs, fieldsStream.getFilePointer(), metaStream);//调用indexWriter.flush,将indexWriter的压缩数据写到真实的fieldsStreama,metaStream文件
    metaStream.writeVLong(numChunks);
    metaStream.writeVLong(numDirtyChunks);
    metaStream.writeVLong(numDirtyDocs);
    CodecUtil.writeFooter(metaStream);
    CodecUtil.writeFooter(fieldsStream);
    assert bufferedDocs.size() == 0;
  }

  // bulk merge is scary: its caused corruption bugs in the past.
  // we try to be extra safe with this impl, but add an escape hatch to
  // have a workaround for undiscovered bugs.
  static final String BULK_MERGE_ENABLED_SYSPROP =
      Lucene90CompressingStoredFieldsWriter.class.getName() + ".enableBulkMerge";
  static final boolean BULK_MERGE_ENABLED;

  static {
    boolean v = true;
    try {
      v = Boolean.parseBoolean(System.getProperty(BULK_MERGE_ENABLED_SYSPROP, "true"));
    } catch (
        @SuppressWarnings("unused")
        SecurityException ignored) {
    }
    BULK_MERGE_ENABLED = v;
  }

  private void copyOneDoc(Lucene90CompressingStoredFieldsReader reader, int docID)
      throws IOException {
    assert reader.getVersion() == VERSION_CURRENT;
    SerializedDocument doc = reader.document(docID);
    startDocument();
    bufferedDocs.copyBytes(doc.in, doc.length);
    numStoredFieldsInDoc = doc.numStoredFields;
    finishDocument();
  }

  private void copyChunks(
      final MergeState mergeState,
      final CompressingStoredFieldsMergeSub sub,
      final int fromDocID,
      final int toDocID)
      throws IOException {
    final Lucene90CompressingStoredFieldsReader reader =
        (Lucene90CompressingStoredFieldsReader) mergeState.storedFieldsReaders[sub.readerIndex];
    assert reader.getVersion() == VERSION_CURRENT;
    assert reader.getChunkSize() == chunkSize;
    assert reader.getCompressionMode() == compressionMode;
    assert !tooDirty(reader);
    assert mergeState.liveDocs[sub.readerIndex] == null;

    int docID = fromDocID;
    final FieldsIndex index = reader.getIndexReader();

    // copy docs that belong to the previous chunk
    while (docID < toDocID && reader.isLoaded(docID)) {
      copyOneDoc(reader, docID++);
    }
    if (docID >= toDocID) {
      return;
    }
    // copy chunks
    long fromPointer = index.getStartPointer(docID);
    final long toPointer =
        toDocID == sub.maxDoc ? reader.getMaxPointer() : index.getStartPointer(toDocID);
    if (fromPointer < toPointer) {
      if (numBufferedDocs > 0) {
        flush(true);
      }
      final IndexInput rawDocs = reader.getFieldsStream();
      rawDocs.seek(fromPointer);
      do {
        final int base = rawDocs.readVInt();
        final int code = rawDocs.readVInt();
        final int bufferedDocs = code >>> 2;
        if (base != docID) {
          throw new CorruptIndexException(
              "invalid state: base=" + base + ", docID=" + docID, rawDocs);
        }
        // write a new index entry and new header for this chunk.
        indexWriter.writeIndex(bufferedDocs, fieldsStream.getFilePointer());
        fieldsStream.writeVInt(docBase); // rebase
        fieldsStream.writeVInt(code);
        docID += bufferedDocs;
        docBase += bufferedDocs;
        if (docID > toDocID) {
          throw new CorruptIndexException(
              "invalid state: base=" + base + ", count=" + bufferedDocs + ", toDocID=" + toDocID,
              rawDocs);
        }
        // copy bytes until the next chunk boundary (or end of chunk data).
        // using the stored fields index for this isn't the most efficient, but fast enough
        // and is a source of redundancy for detecting bad things.
        final long endChunkPointer;
        if (docID == sub.maxDoc) {
          endChunkPointer = reader.getMaxPointer();
        } else {
          endChunkPointer = index.getStartPointer(docID);
        }
        fieldsStream.copyBytes(rawDocs, endChunkPointer - rawDocs.getFilePointer());
        ++numChunks;
        final boolean dirtyChunk = (code & 2) != 0;
        if (dirtyChunk) {
          assert bufferedDocs < maxDocsPerChunk;
          ++numDirtyChunks;
          numDirtyDocs += bufferedDocs;
        }
        fromPointer = endChunkPointer;
      } while (fromPointer < toPointer);
    }

    // copy leftover docs that don't form a complete chunk
    assert reader.isLoaded(docID) == false;
    while (docID < toDocID) {
      copyOneDoc(reader, docID++);
    }
  }

  @Override
  public int merge(MergeState mergeState) throws IOException {
    final MatchingReaders matchingReaders = new MatchingReaders(mergeState);
    final MergeVisitor[] visitors = new MergeVisitor[mergeState.storedFieldsReaders.length];
    final List<CompressingStoredFieldsMergeSub> subs =
        new ArrayList<>(mergeState.storedFieldsReaders.length);
    for (int i = 0; i < mergeState.storedFieldsReaders.length; i++) {
      final StoredFieldsReader reader = mergeState.storedFieldsReaders[i];
      reader.checkIntegrity();
      MergeStrategy mergeStrategy = getMergeStrategy(mergeState, matchingReaders, i);
      if (mergeStrategy == MergeStrategy.VISITOR) {
        visitors[i] = new MergeVisitor(mergeState, i);
      }
      subs.add(new CompressingStoredFieldsMergeSub(mergeState, mergeStrategy, i));
    }
    int docCount = 0;
    final DocIDMerger<CompressingStoredFieldsMergeSub> docIDMerger =
        DocIDMerger.of(subs, mergeState.needsIndexSort);
    CompressingStoredFieldsMergeSub sub = docIDMerger.next();
    while (sub != null) {
      assert sub.mappedDocID == docCount : sub.mappedDocID + " != " + docCount;
      final StoredFieldsReader reader = mergeState.storedFieldsReaders[sub.readerIndex];
      if (sub.mergeStrategy == MergeStrategy.BULK) {
        final int fromDocID = sub.docID;
        int toDocID = fromDocID;
        final CompressingStoredFieldsMergeSub current = sub;
        while ((sub = docIDMerger.next()) == current) {
          ++toDocID;
          assert sub.docID == toDocID;
        }
        ++toDocID; // exclusive bound
        copyChunks(mergeState, current, fromDocID, toDocID);
        docCount += (toDocID - fromDocID);
      } else if (sub.mergeStrategy == MergeStrategy.DOC) {
        copyOneDoc((Lucene90CompressingStoredFieldsReader) reader, sub.docID);
        ++docCount;
        sub = docIDMerger.next();
      } else if (sub.mergeStrategy == MergeStrategy.VISITOR) {
        assert visitors[sub.readerIndex] != null;
        startDocument();
        reader.visitDocument(sub.docID, visitors[sub.readerIndex]);
        finishDocument();
        ++docCount;
        sub = docIDMerger.next();
      } else {
        throw new AssertionError("Unknown merge strategy [" + sub.mergeStrategy + "]");
      }
    }
    finish(docCount);
    return docCount;
  }

  /**
   * Returns true if we should recompress this reader, even though we could bulk merge compressed
   * data
   *
   * <p>The last chunk written for a segment is typically incomplete, so without recompressing, in
   * some worst-case situations (e.g. frequent reopen with tiny flushes), over time the compression
   * ratio can degrade. This is a safety switch.
   */
  boolean tooDirty(Lucene90CompressingStoredFieldsReader candidate) {
    // A segment is considered dirty only if it has enough dirty docs to make a full block
    // AND more than 1% blocks are dirty.
    return candidate.getNumDirtyDocs() > maxDocsPerChunk
        && candidate.getNumDirtyChunks() * 100 > candidate.getNumChunks();
  }

  private enum MergeStrategy {
    /** Copy chunk by chunk in a compressed format */
    BULK,

    /** Copy document by document in a decompressed format */
    DOC,

    /** Copy field by field of decompressed documents */
    VISITOR
  }

  private MergeStrategy getMergeStrategy(
      MergeState mergeState, MatchingReaders matchingReaders, int readerIndex) {
    final StoredFieldsReader candidate = mergeState.storedFieldsReaders[readerIndex];
    if (matchingReaders.matchingReaders[readerIndex] == false
        || candidate instanceof Lucene90CompressingStoredFieldsReader == false
        || ((Lucene90CompressingStoredFieldsReader) candidate).getVersion() != VERSION_CURRENT) {
      return MergeStrategy.VISITOR;
    }
    Lucene90CompressingStoredFieldsReader reader =
        (Lucene90CompressingStoredFieldsReader) candidate;
    if (BULK_MERGE_ENABLED
        && reader.getCompressionMode() == compressionMode
        && reader.getChunkSize() == chunkSize
        // its not worth fine-graining this if there are deletions.
        && mergeState.liveDocs[readerIndex] == null
        && !tooDirty(reader)) {
      return MergeStrategy.BULK;
    } else {
      return MergeStrategy.DOC;
    }
  }

  private static class CompressingStoredFieldsMergeSub extends DocIDMerger.Sub {
    private final int readerIndex;
    private final int maxDoc;
    private final MergeStrategy mergeStrategy;
    int docID = -1;

    CompressingStoredFieldsMergeSub(
        MergeState mergeState, MergeStrategy mergeStrategy, int readerIndex) {
      super(mergeState.docMaps[readerIndex]);
      this.readerIndex = readerIndex;
      this.mergeStrategy = mergeStrategy;
      this.maxDoc = mergeState.maxDocs[readerIndex];
    }

    @Override
    public int nextDoc() {
      docID++;
      if (docID == maxDoc) {
        return NO_MORE_DOCS;
      } else {
        return docID;
      }
    }
  }

  @Override
  public long ramBytesUsed() {
    return bufferedDocs.ramBytesUsed()
        + numStoredFields.length * Integer.BYTES
        + endOffsets.length * Integer.BYTES;
  }
}
