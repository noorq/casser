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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import net.helenus.core.Query;
import org.junit.Assert;
import org.junit.Test;

public class TupleMapTest extends TupleCollectionTest {

  @Test
  public void testMapCRUID() throws TimeoutException {

    int id = 333;

    Map<Section, Author> writers = new HashMap<Section, Author>();
    writers.put(
        new SectionImpl("first", 1), new TupleCollectionTest.AuthorImpl("Alex", "San Jose"));
    writers.put(
        new SectionImpl("second", 2), new TupleCollectionTest.AuthorImpl("Bob", "San Francisco"));

    // CREATE

    session.insert().value(book::id, id).value(book::writers, writers).sync();

    // READ

    // read full object

    Book actual =
        session.<Book>select(book).where(book::id, Query.eq(id)).single().sync().orElse(null);
    Assert.assertEquals(id, actual.id());
    assertEqualMaps(writers, actual.writers());
    Assert.assertNull(actual.reviewers());
    Assert.assertNull(actual.notes());
    Assert.assertNull(actual.contents());

    // read full map

    Map<Section, Author> actualMap =
        session.select(book::writers).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualMaps(writers, actualMap);

    // read single key-value in map

    String cql =
        session
            .select(Query.get(book::writers, new SectionImpl("first", 1)))
            .where(book::id, Query.eq(id))
            .cql();

    System.out.println("Still not supporting cql = " + cql);

    // UPDATE

    Map<Section, Author> expected = new HashMap<Section, Author>();
    expected.put(new SectionImpl("f", 1), new TupleCollectionTest.AuthorImpl("A", "SJ"));
    expected.put(new SectionImpl("s", 1), new TupleCollectionTest.AuthorImpl("B", "SF"));

    session.update().set(book::writers, expected).where(book::id, Query.eq(id)).sync();

    actual = session.<Book>select(book).where(book::id, Query.eq(id)).single().sync().orElse(null);

    Assert.assertEquals(id, actual.id());
    assertEqualMaps(expected, actual.writers());

    // INSERT

    // put operation

    Section third = new SectionImpl("t", 3);
    Author unk = new TupleCollectionTest.AuthorImpl("Unk", "City 17");

    expected.put(third, unk);
    session.update().put(book::writers, third, unk).where(book::id, Query.eq(id)).sync();

    actualMap =
        session.select(book::writers).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualMaps(expected, actualMap);

    // putAll operation
    expected.putAll(writers);
    session.update().putAll(book::writers, writers).where(book::id, Query.eq(id)).sync();

    actualMap =
        session.select(book::writers).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualMaps(expected, actualMap);

    // put existing

    expected.put(third, unk);
    session.update().put(book::writers, third, unk).where(book::id, Query.eq(id)).sync();

    actualMap =
        session.select(book::writers).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualMaps(expected, actualMap);

    // DELETE

    // remove single key

    expected.remove(third);
    session.update().put(book::writers, third, null).where(book::id, Query.eq(id)).sync();

    actualMap =
        session.select(book::writers).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualMaps(expected, actualMap);

    // remove full map

    session.update().set(book::writers, null).where(book::id, Query.eq(id)).sync();

    actualMap =
        session.select(book::writers).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    Assert.assertNull(actualMap);

    // remove object

    session.delete().where(book::id, Query.eq(id)).sync();
    Long cnt = session.count().where(book::id, Query.eq(id)).sync();
    Assert.assertEquals(Long.valueOf(0), cnt);
  }

  private void assertEqualMaps(Map<Section, Author> expected, Map<Section, Author> actual) {

    Assert.assertEquals(expected.size(), actual.size());

    for (Section e : expected.keySet()) {
      Section a =
          actual.keySet().stream().filter(p -> p.title().equals(e.title())).findFirst().get();
      Assert.assertEquals(e.title(), a.title());
      Assert.assertEquals(e.page(), a.page());

      Author ea = expected.get(e);
      Author aa = actual.get(a);

      Assert.assertEquals(ea.name(), aa.name());
      Assert.assertEquals(ea.city(), aa.city());
    }
  }
}
