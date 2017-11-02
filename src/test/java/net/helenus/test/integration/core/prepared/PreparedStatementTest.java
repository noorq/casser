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
package net.helenus.test.integration.core.prepared;

import com.datastax.driver.core.ResultSet;
import java.math.BigDecimal;
import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.core.Query;
import net.helenus.core.operation.PreparedOperation;
import net.helenus.core.operation.PreparedStreamOperation;
import net.helenus.support.Fun;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class PreparedStatementTest extends AbstractEmbeddedCassandraTest {

  static Car car;

  static HelenusSession session;

  static PreparedOperation<ResultSet> insertOp;

  static PreparedOperation<ResultSet> updateOp;

  static PreparedStreamOperation<Car> selectOp;

  static PreparedStreamOperation<Fun.Tuple1<BigDecimal>> selectPriceOp;

  static PreparedOperation<ResultSet> deleteOp;

  static PreparedOperation<Long> countOp;

  @BeforeClass
  public static void beforeTest() {

    session = Helenus.init(getSession()).showCql().add(Car.class).autoCreateDrop().get();
    car = Helenus.dsl(Car.class, session.getMetadata());

    insertOp =
        session
            .<ResultSet>insert()
            .value(car::make, Query.marker())
            .value(car::model, Query.marker())
            .value(car::year, 2004)
            .prepare();

    updateOp =
        session
            .update()
            .set(car::price, Query.marker())
            .where(car::make, Query.eq(Query.marker()))
            .and(car::model, Query.eq(Query.marker()))
            .prepare();

    selectOp =
        session
            .<Car>select(car)
            .where(car::make, Query.eq(Query.marker()))
            .and(car::model, Query.eq(Query.marker()))
            .prepare();

    selectPriceOp =
        session
            .select(car::price)
            .where(car::make, Query.eq(Query.marker()))
            .and(car::model, Query.eq(Query.marker()))
            .prepare();

    deleteOp =
        session
            .delete()
            .where(car::make, Query.eq(Query.marker()))
            .and(car::model, Query.eq(Query.marker()))
            .prepare();

    countOp =
        session
            .count()
            .where(car::make, Query.eq(Query.marker()))
            .and(car::model, Query.eq(Query.marker()))
            .prepare();
  }

  @Test
  public void testPrint() {
    System.out.println(car);
  }

  @Test
  public void testCRUID() throws Exception {

    // INSERT

    insertOp.bind("Nissan", "350Z").sync();

    // SELECT

    Car actual = selectOp.bind("Nissan", "350Z").sync().findFirst().get();
    Assert.assertEquals("Nissan", actual.make());
    Assert.assertEquals("350Z", actual.model());
    Assert.assertEquals(2004, actual.year());
    Assert.assertNull(actual.price());

    // UPDATE

    updateOp.bind(BigDecimal.valueOf(10000.0), "Nissan", "350Z").sync();

    BigDecimal price = selectPriceOp.bind("Nissan", "350Z").sync().findFirst().get()._1;

    Assert.assertEquals(BigDecimal.valueOf(10000.0), price);

    // DELETE

    Long cnt = countOp.bind("Nissan", "350Z").sync();
    Assert.assertEquals(Long.valueOf(1), cnt);

    deleteOp.bind("Nissan", "350Z").sync();

    cnt = countOp.bind("Nissan", "350Z").sync();
    Assert.assertEquals(Long.valueOf(0), cnt);
  }
}
