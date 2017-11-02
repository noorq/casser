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
package net.helenus.test.integration.core.udtcollection;

import static net.helenus.core.Query.eq;
import static net.helenus.core.Query.get;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import org.junit.Assert;
import org.junit.Test;

public class UDTValueMapTest extends UDTCollectionTest {

  @Test
  public void testValueMapCRUID() throws TimeoutException {

    int id = 999;

    Map<Integer, Section> contents = new HashMap<Integer, Section>();
    contents.put(1, new SectionImpl("first", 1));
    contents.put(2, new SectionImpl("second", 2));

    // CREATE

    session.insert().value(book::id, id).value(book::contents, contents).sync();

    // READ

    // read full object

    Book actual = session.select(Book.class).where(book::id, eq(id)).sync().findFirst().get();
    Assert.assertEquals(id, actual.id());
    assertEqualMaps(contents, actual.contents());
    Assert.assertNull(actual.reviewers());
    Assert.assertNull(actual.writers());
    Assert.assertNull(actual.notes());

    // read full map

    Map<Integer, Section> actualMap =
        session.select(book::contents).where(book::id, eq(id)).sync().findFirst().get()._1;
    assertEqualMaps(contents, actualMap);

    // read single key-value in map

    String cql = session.select(get(book::contents, 1)).where(book::id, eq(id)).cql();

    System.out.println("Still not supporting cql = " + cql);

    // UPDATE

    Map<Integer, Section> expected = new HashMap<Integer, Section>();
    expected.put(4, new SectionImpl("4", 4));
    expected.put(5, new SectionImpl("5", 5));

    session.update().set(book::contents, expected).where(book::id, eq(id)).sync();

    actual = session.select(Book.class).where(book::id, eq(id)).sync().findFirst().get();
    Assert.assertEquals(id, actual.id());
    assertEqualMaps(expected, actual.contents());

    // INSERT

    // put operation

    Section third = new SectionImpl("t", 3);

    expected.put(3, third);
    session.update().put(book::contents, 3, third).where(book::id, eq(id)).sync();

    actualMap = session.select(book::contents).where(book::id, eq(id)).sync().findFirst().get()._1;
    assertEqualMaps(expected, actualMap);

    // putAll operation
    expected.putAll(contents);
    session.update().putAll(book::contents, contents).where(book::id, eq(id)).sync();

    actualMap = session.select(book::contents).where(book::id, eq(id)).sync().findFirst().get()._1;
    assertEqualMaps(expected, actualMap);

    // put existing

    third = new SectionImpl("t-replace", 3);
    expected.put(3, third);
    session.update().put(book::contents, 3, third).where(book::id, eq(id)).sync();

    actualMap = session.select(book::contents).where(book::id, eq(id)).sync().findFirst().get()._1;
    assertEqualMaps(expected, actualMap);

    // DELETE

    // remove single key

    expected.remove(3);
    session.update().put(book::contents, 3, null).where(book::id, eq(id)).sync();

    actualMap = session.select(book::contents).where(book::id, eq(id)).sync().findFirst().get()._1;
    assertEqualMaps(expected, actualMap);

    // remove full map

    session.update().set(book::contents, null).where(book::id, eq(id)).sync();

    actualMap = session.select(book::contents).where(book::id, eq(id)).sync().findFirst().get()._1;
    Assert.assertNull(actualMap);

    // remove object

    session.delete().where(book::id, eq(id)).sync();
    Long cnt = session.count().where(book::id, eq(id)).sync();
    Assert.assertEquals(Long.valueOf(0), cnt);
  }

  private void assertEqualMaps(Map<Integer, Section> expected, Map<Integer, Section> actual) {

    Assert.assertEquals(expected.size(), actual.size());

    for (Integer i : expected.keySet()) {
      Section e = expected.get(i);
      Section a = actual.get(i);
      Assert.assertEquals(e.title(), a.title());
      Assert.assertEquals(e.page(), a.page());
    }
  }
}
