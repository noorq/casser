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
package net.helenus.test.integration.core.index;

import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.core.Query;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.TimeoutException;

public class SecondaryIndexTest extends AbstractEmbeddedCassandraTest {

  Book book;

  HelenusSession session;

  @Before
  public void beforeTest() {
    session = Helenus.init(getSession()).showCql().add(Book.class).autoCreateDrop().get();
    book = Helenus.dsl(Book.class, session.getMetadata());
  }

  @Test
  public void test() throws TimeoutException {

    session
        .insert()
        .value(book::id, 123L)
        .value(book::isbn, "ABC")
        .value(book::author, "Alex")
        .sync();

    long actualId =
        session.select(book::id).where(book::isbn, Query.eq("ABC")).sync().findFirst().get()._1;

    Assert.assertEquals(123L, actualId);
  }
}
