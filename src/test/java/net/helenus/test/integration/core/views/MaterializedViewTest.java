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
package net.helenus.test.integration.core.views;

import static net.helenus.core.Query.eq;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import net.helenus.core.ConflictingUnitOfWorkException;
import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.core.UnitOfWork;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

// See: https://docs.datastax.com/en/cql/3.3/cql/cql_using/useCreateMV.html
//      https://docs.datastax.com/en/cql/3.3/cql/cql_reference/cqlCreateMaterializedView.html
//      https://www.datastax.com/dev/blog/materialized-view-performance-in-cassandra-3-x
//      https://cassandra-zone.com/materialized-views/
public class MaterializedViewTest extends AbstractEmbeddedCassandraTest {

  static Cyclist cyclist;
  static HelenusSession session;

  static Date dateFromString(String dateInString) {
    SimpleDateFormat formatter = new SimpleDateFormat("dd-MMM-yyyy");
    try {
      return formatter.parse(dateInString);
    } catch (ParseException e) {
      e.printStackTrace();
    }
    return null;
  }

  @BeforeClass
  public static void beforeTest() {
    session =
        Helenus.init(getSession())
            .showCql()
            .add(Cyclist.class)
            .add(CyclistsByAge.class)
            .autoCreateDrop()
            .get();
    cyclist = session.dsl(Cyclist.class);

    try {
      session
          .insert(cyclist)
          .value(cyclist::cid, UUID.randomUUID())
          .value(cyclist::age, 18)
          .value(cyclist::birthday, dateFromString("1997-02-08"))
          .value(cyclist::country, "Netherlands")
          .value(cyclist::name, "Pascal EENKHOORN")
          .sync();
    } catch (TimeoutException e) {
    }
  }

  @Test
  public void testMv() throws TimeoutException {
    session
        .select(Cyclist.class)
        .from(CyclistsByAge.class)
        .where(cyclist::age, eq(18))
        .allowFiltering()
        .single()
        .sync();
  }

  @Test
  public void testMvUnitOfWork()
      throws TimeoutException, ConflictingUnitOfWorkException, Exception {
    Cyclist c1, c2;

    UnitOfWork uow = session.begin();
    c1 =
        session
            .<Cyclist>select(Cyclist.class)
            .from(CyclistsByAge.class)
            .where(cyclist::age, eq(18))
            .single()
            .sync(uow)
            .orElse(null);

    c2 =
        session
            .<Cyclist>select(Cyclist.class)
            .from(CyclistsByAge.class)
            .where(cyclist::age, eq(18))
            .single()
            .sync(uow)
            .orElse(null);
    Assert.assertEquals(c1, c2);
    uow.commit();
  }
}
