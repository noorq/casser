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
package net.helenus.test.integration.core.counter;

import static net.helenus.core.Query.eq;

import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class CounterTest extends AbstractEmbeddedCassandraTest {

  static Page page;

  static HelenusSession session;

  @BeforeClass
  public static void beforeTest() {
    session = Helenus.init(getSession()).showCql().add(Page.class).autoCreateDrop().get();
    page = Helenus.dsl(Page.class, session.getMetadata());
  }

  @Test
  public void testPrint() {
    System.out.println(page);
  }

  @Test
  public void testCounter() {

    boolean exists =
        session.select(page::hits).where(page::alias, eq("index")).sync().findFirst().isPresent();
    Assert.assertFalse(exists);

    session.update().increment(page::hits, 10L).where(page::alias, eq("index")).sync();

    long hits =
        session.select(page::hits).where(page::alias, eq("index")).sync().findFirst().get()._1;
    Assert.assertEquals(10, hits);

    session.update().decrement(page::hits).where(page::alias, eq("index")).sync();

    hits = session.select(page::hits).where(page::alias, eq("index")).sync().findFirst().get()._1;
    Assert.assertEquals(9, hits);
  }
}
