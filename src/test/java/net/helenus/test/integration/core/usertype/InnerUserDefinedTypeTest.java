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
package net.helenus.test.integration.core.usertype;

import com.google.common.collect.Sets;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.core.Query;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class InnerUserDefinedTypeTest extends AbstractEmbeddedCassandraTest {

  static Customer customer;
  static AddressInformation addressInformation;

  static HelenusSession session;

  @BeforeClass
  public static void beforeTest() {
    Helenus.clearDslCache();
    session = Helenus.init(getSession()).showCql().add(Customer.class).autoCreateDrop().get();
    customer = Helenus.dsl(Customer.class);
    addressInformation = Helenus.dsl(AddressInformation.class);
  }

  @AfterClass
  public static void afterTest() {
    session.getSession().execute("DROP TABLE IF EXISTS customer;");
    session.getSession().execute("DROP TYPE IF EXISTS address_information;");
    //      SchemaUtil.dropUserType(session.getSessionRepository().findUserType("address_information")), true);
  }

  @Test
  public void testPrint() {
    System.out.println(addressInformation);
    System.out.println(customer);
  }

  @Test
  public void testCrud() throws TimeoutException {

    UUID id = UUID.randomUUID();

    Address a =
        new Address() {

          @Override
          public String street() {
            return "1 st";
          }

          @Override
          public String city() {
            return "San Jose";
          }

          @Override
          public int zip() {
            return 95131;
          }

          @Override
          public String country() {
            return "USA";
          }

          @Override
          public Set<String> phones() {
            return Sets.newHashSet("14080000000");
          }
        };

    AddressInformation ai =
        new AddressInformation() {

          @Override
          public Address address() {
            return a;
          }
        };

    session.insert().value(customer::id, id).value(customer::addressInformation, ai).sync();

    String cql =
        session
            .update()
            .set(customer.addressInformation().address()::street, "3 st")
            .where(customer::id, Query.eq(id))
            .cql();

    //TODO: System.out.println("At the time when this test was written Cassandra did not support queries like this: " + cql);

    session.update().set(customer::addressInformation, ai).where(customer::id, Query.eq(id)).sync();

    String street =
        session
            .select(customer.addressInformation().address()::street)
            .where(customer::id, Query.eq(id))
            .sync()
            .findFirst()
            .get()
            ._1;

    Assert.assertEquals("1 st", street);

    session.delete().where(customer::id, Query.eq(id)).sync();

    Long cnt = session.count().where(customer::id, Query.eq(id)).sync();

    Assert.assertEquals(Long.valueOf(0), cnt);
  }
}
