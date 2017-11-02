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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeoutException;
import net.helenus.core.Query;
import org.junit.Assert;
import org.junit.Test;

public class UDTSetTest extends UDTCollectionTest {

  @Test
  public void testSetCRUID() throws TimeoutException {

    int id = 555;

    // CREATE

    Set<Author> reviewers = new HashSet<Author>();
    reviewers.add(new AuthorImpl("Alex", "San Jose"));
    reviewers.add(new AuthorImpl("Bob", "San Francisco"));

    session.insert().value(book::id, id).value(book::reviewers, reviewers).sync();

    // READ

    Book actual = session.select(Book.class).where(book::id, Query.eq(id)).sync().findFirst().get();
    Assert.assertEquals(id, actual.id());
    assertEqualSets(reviewers, actual.reviewers());

    // UPDATE

    Set<Author> expected = new HashSet<Author>();
    expected.add(new AuthorImpl("Craig", "Los Altos"));

    session.update().set(book::reviewers, expected).where(book::id, Query.eq(id)).sync();

    Set<Author> actualSet =
        session.select(book::reviewers).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualSets(expected, actualSet);

    // add operation

    expected.add(new AuthorImpl("Add", "AddCity"));
    session
        .update()
        .add(book::reviewers, new AuthorImpl("Add", "AddCity"))
        .where(book::id, Query.eq(id))
        .sync();

    actualSet =
        session.select(book::reviewers).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualSets(expected, actualSet);

    // addAll operation
    expected.addAll(reviewers);
    session.update().addAll(book::reviewers, reviewers).where(book::id, Query.eq(id)).sync();

    actualSet =
        session.select(book::reviewers).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualSets(expected, actualSet);

    // DELETE

    // remove single value

    Author a = expected.stream().filter(p -> p.name().equals("Add")).findFirst().get();
    expected.remove(a);

    session.update().remove(book::reviewers, a).where(book::id, Query.eq(id)).sync();

    actualSet =
        session.select(book::reviewers).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualSets(expected, actualSet);

    // remove values

    expected.remove(expected.stream().filter(p -> p.name().equals("Alex")).findFirst().get());
    expected.remove(expected.stream().filter(p -> p.name().equals("Bob")).findFirst().get());
    session.update().removeAll(book::reviewers, reviewers).where(book::id, Query.eq(id)).sync();

    actualSet =
        session.select(book::reviewers).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    assertEqualSets(expected, actualSet);

    // remove full list

    session.update().set(book::reviewers, null).where(book::id, Query.eq(id)).sync();

    actualSet =
        session.select(book::reviewers).where(book::id, Query.eq(id)).sync().findFirst().get()._1;
    Assert.assertNull(actualSet);

    // remove object

    session.delete().where(book::id, Query.eq(id)).sync();
    Long cnt = session.count().where(book::id, Query.eq(id)).sync();
    Assert.assertEquals(Long.valueOf(0), cnt);
  }

  private void assertEqualSets(Set<Author> expected, Set<Author> actual) {
    Assert.assertEquals(expected.size(), actual.size());

    for (Author e : expected) {
      Author a = actual.stream().filter(p -> p.name().equals(e.name())).findFirst().get();
      Assert.assertEquals(e.city(), a.city());
    }
  }
}
