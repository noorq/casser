/*
 *      Copyright (C) 2015 The Helenus Authors
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package net.helenus.test.integration.core.tuplecollection;

import java.util.ArrayList;
import java.util.List;
import net.helenus.core.Query;
import org.junit.Assert;
import org.junit.Test;

public class TupleListTest extends TupleCollectionTest {

  @Test
  public void testListCRUID() {

    int id = 777;

    List<Author> authors = new ArrayList<Author>();
    authors.add(new AuthorImpl("Alex", "San Jose"));
    authors.add(new AuthorImpl("Bob", "San Francisco"));

    // CREATE

    session.insert().value(book::id, id).value(book::authors, authors).sync();

    // READ

    // read full object

    Book actual = session.select(Book.class).where(book::id, Query.eq(id)).sync().findFirst().get();
    Assert.assertEquals(id, actual.id());
    assertEqualLists(authors, actual.authors());
    Assert.assertNull(actual.reviewers());
    Assert.assertNull(actual.contents());

    // read full list

    List<Author> actualList =
        session.select(book::authors).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualLists(authors, actualList);

    // read single value by index

    String cql = session.select(Query.getIdx(book::authors, 1)).where(book::id, Query.eq(id)).cql();

    System.out.println("Still not supporting cql = " + cql);

    // UPDATE

    List<Author> expected = new ArrayList<Author>();
    expected.add(new AuthorImpl("Unknown", "City 17"));

    session.update().set(book::authors, expected).where(book::id, Query.eq(id)).sync();

    actual = session.select(Book.class).where(book::id, Query.eq(id)).sync().findFirst().get();
    Assert.assertEquals(id, actual.id());
    assertEqualLists(expected, actual.authors());

    // INSERT

    // prepend operation

    expected.add(0, new AuthorImpl("Prepend", "PrependCity"));
    session
        .update()
        .prepend(book::authors, new AuthorImpl("Prepend", "PrependCity"))
        .where(book::id, Query.eq(id))
        .sync();

    actualList =
        session.select(book::authors).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualLists(expected, actualList);

    // append operation

    expected.add(new AuthorImpl("Append", "AppendCity"));
    session
        .update()
        .append(book::authors, new AuthorImpl("Append", "AppendCity"))
        .where(book::id, Query.eq(id))
        .sync();

    actualList =
        session.select(book::authors).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualLists(expected, actualList);

    // prependAll operation
    expected.addAll(0, authors);
    session.update().prependAll(book::authors, authors).where(book::id, Query.eq(id)).sync();

    actualList =
        session.select(book::authors).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualLists(expected, actualList);

    // appendAll operation
    expected.addAll(authors);
    session.update().appendAll(book::authors, authors).where(book::id, Query.eq(id)).sync();

    actualList =
        session.select(book::authors).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualLists(expected, actualList);

    // set by Index

    Author inserted = new AuthorImpl("Insert", "InsertCity");
    expected.set(5, inserted);
    session.update().setIdx(book::authors, 5, inserted).where(book::id, Query.eq(id)).sync();

    actualList =
        session.select(book::authors).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualLists(expected, actualList);

    // DELETE

    // remove single value

    expected.remove(inserted);
    session.update().discard(book::authors, inserted).where(book::id, Query.eq(id)).sync();

    actualList =
        session.select(book::authors).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualLists(expected, actualList);

    // remove values

    expected.removeAll(authors);
    session.update().discardAll(book::authors, authors).where(book::id, Query.eq(id)).sync();

    actualList =
        session.select(book::authors).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualLists(expected, actualList);

    // remove full list

    session.update().set(book::authors, null).where(book::id, Query.eq(id)).sync();

    actualList =
        session.select(book::authors).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    Assert.assertNull(actualList);

    // remove object

    session.delete().where(book::id, Query.eq(id)).sync();
    Long cnt = session.count().where(book::id, Query.eq(id)).sync();
    Assert.assertEquals(Long.valueOf(0), cnt);
  }

  private void assertEqualLists(List<Author> expected, List<Author> actual) {
    Assert.assertEquals(expected.size(), actual.size());

    int size = expected.size();

    for (int i = 0; i != size; ++i) {
      Author e = expected.get(i);
      Author a = actual.get(i);
      Assert.assertEquals(e.name(), a.name());
      Assert.assertEquals(e.city(), a.city());
    }
  }
}
