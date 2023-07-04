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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.MappedMultiFields;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.ReaderSlice;

/**
 * Abstract API that consumes terms, doc, freq, prox, offset and payloads postings. Concrete
 * implementations of this actually do "something" with the postings (write it into the index in a
 * specific format).抽象类： 消费内存里构建的terms，doc，freq prox等信息， 然后调用具体的持久化writer
 * 流程： FieldsWriter(codecs.perfield.PerFieldPostingsFormat.FieldsWriter)根据当前segment信息生成一个专门消费内存里的倒排数据结构的consumer:
 * @lucene.experimental xxxxBlockTreeTermsWriter,它会初始化并开始创建对应的落盘文件，接着FieldsConsumer的子类FieldsWriter会调用
 *///xxxBlockTreeTermsWriter的writer方法写所有的Fields；xxxBlockTreeTermsWriter会遍历Fields的每个Field的所有terms，然后利用terms下的迭代器：
public abstract class FieldsConsumer implements Closeable {// termsEnum来遍历所有的term，然后调用其局部类：TermsWriter的writer(Term,
  // TermsEnum)方法进行对这个term的写入，写入完毕后生成termDict，.tip和.tim生成完毕。BlockTreeTermsWriter下的TermsWriter负责负责整个倒排链的写入， 包括.doc,.pos, .pay的生成，这个过程需要借助LucenePostingsWriter来辅助完成
  /** Sole constructor. (For invocation by subclass constructors, typically implicit.) */
  protected FieldsConsumer() {}

  // TODO: can we somehow compute stats for you...?

  // TODO: maybe we should factor out "limited" (only
  // iterables, no counts/stats) base classes from
  // Fields/Terms/Docs/AndPositions?

  /**
   * Write all fields, terms and postings. This the "pull" API, allowing you to iterate more than
   * once over the postings, somewhat analogous to using a DOM API to traverse an XML tree.
   * // 拉取式api
   * <p><b>Notes</b>:
   *
   * <ul> // 要计算索引的统计数据， 包括全部的sum
   *   <li>You must compute index statistics, including each Term's docFreq and totalTermFreq, as
   *       well as the summary sumTotalTermFreq, sumTotalDocFreq and docCount.
   *   <li>You must skip terms that have no docs and fields that have no terms, even though the
   *       provided Fields API will expose them; this typically requires lazily writing the field or
   *       term until you've actually seen the first term or document. // 至于哪些没有实际的doc，甚至terms的field，暂时不写，lazy的方式，有了再写
   *   <li>The provided Fields instance is limited: you cannot call any methods that return
   *       statistics/counts; you cannot pass a non-null live docs when pulling docs/positions
   *       enums. //
   * </ul>
   */// org.apache.lucene.index.FreqProxTermsWriter.flush 调用
  public abstract void write(Fields fields, NormsProducer norms) throws IOException;

  /**
   * Merges in the fields from the readers in <code>mergeState</code>. The default implementation
   * skips and maps around deleted documents, and calls {@link #write(Fields,NormsProducer)}.
   * Implementations can override this method for more sophisticated merging (bulk-byte copying,
   * etc).
   */
  public void merge(MergeState mergeState, NormsProducer norms) throws IOException {
    final List<Fields> fields = new ArrayList<>();
    final List<ReaderSlice> slices = new ArrayList<>();

    int docBase = 0;

    for (int readerIndex = 0; readerIndex < mergeState.fieldsProducers.length; readerIndex++) {
      final FieldsProducer f = mergeState.fieldsProducers[readerIndex];

      final int maxDoc = mergeState.maxDocs[readerIndex];
      if (f != null) {
        f.checkIntegrity();
        slices.add(new ReaderSlice(docBase, maxDoc, readerIndex));
        fields.add(f);
      }
      docBase += maxDoc;
    }

    Fields mergedFields =
        new MappedMultiFields(
            mergeState,
            new MultiFields(
                fields.toArray(Fields.EMPTY_ARRAY), slices.toArray(ReaderSlice.EMPTY_ARRAY)));
    write(mergedFields, norms);
  }

  // NOTE: strange but necessary so javadocs linting is happy:
  @Override
  public abstract void close() throws IOException;
}
