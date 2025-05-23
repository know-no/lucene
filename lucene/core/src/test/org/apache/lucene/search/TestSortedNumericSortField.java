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
package org.apache.lucene.search;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoubleField;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.Field.Store;
import org.apache.lucene.document.FloatField;
import org.apache.lucene.document.IntField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.apache.lucene.tests.util.LuceneTestCase;

/** Simple tests for SortedNumericSortField */
public class TestSortedNumericSortField extends LuceneTestCase {

  public void testEmptyIndex() throws Exception {
    IndexSearcher empty = newSearcher(new MultiReader());
    Query query = new TermQuery(new Term("contents", "foo"));

    TopDocs td =
        empty.search(
            query,
            10,
            new Sort(new SortedNumericSortField("sortednumeric", SortField.Type.LONG)),
            true);
    assertEquals(0, td.totalHits.value());

    // for an empty index, any selector should work
    for (SortedNumericSelector.Type v : SortedNumericSelector.Type.values()) {
      td =
          empty.search(
              query,
              10,
              new Sort(new SortedNumericSortField("sortednumeric", SortField.Type.LONG, false, v)),
              true);
      assertEquals(0, td.totalHits.value());
    }
  }

  @SuppressWarnings({"unlikely-arg-type", "SelfAssertion"})
  public void testEquals() throws Exception {
    SortField sf = new SortedNumericSortField("a", SortField.Type.LONG);
    assertFalse(sf.equals(null));

    assertEquals(sf, sf);

    SortField sf2 = new SortedNumericSortField("a", SortField.Type.LONG);
    assertEquals(sf, sf2);
    assertEquals(sf.hashCode(), sf2.hashCode());

    assertFalse(sf.equals(new SortedNumericSortField("a", SortField.Type.LONG, true)));
    assertFalse(sf.equals(new SortedNumericSortField("a", SortField.Type.FLOAT)));
    assertFalse(sf.equals(new SortedNumericSortField("b", SortField.Type.LONG)));
    assertFalse(
        sf.equals(
            new SortedNumericSortField(
                "a", SortField.Type.LONG, false, SortedNumericSelector.Type.MAX)));
    assertFalse(sf.equals("foo"));
  }

  public void testForward() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new IntField("value", 5, Store.NO));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new IntField("value", 3, Store.NO));
    doc.add(new IntField("value", 7, Store.NO));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortedNumericSortField("value", SortField.Type.INT));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value());
    // 3 comes before 5
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testReverse() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new IntField("value", 3, Store.NO));
    doc.add(new IntField("value", 7, Store.NO));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new IntField("value", 5, Store.NO));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);

    IndexReader ir = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortedNumericSortField("value", SortField.Type.INT, true));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value());
    // 'bar' comes before 'baz'
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMissingFirst() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new IntField("value", 5, Store.NO));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new IntField("value", 3, Store.NO));
    doc.add(new IntField("value", 7, Store.NO));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(ir);
    SortField sortField = new SortedNumericSortField("value", SortField.Type.INT);
    sortField.setMissingValue(Integer.MIN_VALUE);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value());
    // 3 comes before 5
    // null comes first
    assertEquals("3", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[2].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testMissingLast() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new IntField("value", 5, Store.NO));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new IntField("value", 3, Store.NO));
    doc.add(new IntField("value", 7, Store.NO));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(newStringField("id", "3", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(ir);
    SortField sortField = new SortedNumericSortField("value", SortField.Type.INT);
    sortField.setMissingValue(Integer.MAX_VALUE);
    Sort sort = new Sort(sortField);

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(3, td.totalHits.value());
    // 3 comes before 5
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));
    // null comes last
    assertEquals("3", searcher.storedFields().document(td.scoreDocs[2].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testSingleton() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new IntField("value", 5, Store.NO));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new IntField("value", 3, Store.NO));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortedNumericSortField("value", SortField.Type.INT));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value());
    // 3 comes before 5
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testFloat() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new FloatField("value", -3f, Store.NO));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new FloatField("value", -5f, Store.NO));
    doc.add(new FloatField("value", 7f, Store.NO));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortedNumericSortField("value", SortField.Type.FLOAT));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value());
    // -5 comes before -3
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }

  public void testDouble() throws Exception {
    Directory dir = newDirectory();
    RandomIndexWriter writer = new RandomIndexWriter(random(), dir);
    Document doc = new Document();
    doc.add(new DoubleField("value", -3d, Field.Store.YES));
    doc.add(newStringField("id", "2", Field.Store.YES));
    writer.addDocument(doc);
    doc = new Document();
    doc.add(new DoubleField("value", -5d, Field.Store.YES));
    doc.add(new DoubleField("value", 7d, Field.Store.YES));
    doc.add(newStringField("id", "1", Field.Store.YES));
    writer.addDocument(doc);
    IndexReader ir = writer.getReader();
    writer.close();

    IndexSearcher searcher = newSearcher(ir);
    Sort sort = new Sort(new SortedNumericSortField("value", SortField.Type.DOUBLE));

    TopDocs td = searcher.search(new MatchAllDocsQuery(), 10, sort);
    assertEquals(2, td.totalHits.value());
    // -5 comes before -3
    assertEquals("1", searcher.storedFields().document(td.scoreDocs[0].doc).get("id"));
    assertEquals("2", searcher.storedFields().document(td.scoreDocs[1].doc).get("id"));

    ir.close();
    dir.close();
  }
}
