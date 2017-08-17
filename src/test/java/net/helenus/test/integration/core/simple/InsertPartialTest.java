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
package net.helenus.test.integration.core.simple;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.core.operation.InsertOperation;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class InsertPartialTest extends AbstractEmbeddedCassandraTest {

  static HelenusSession session;
  static User user;
  static Random rnd = new Random();

  @BeforeClass
  public static void beforeTests() {
    session = Helenus.init(getSession()).showCql().add(User.class).autoCreateDrop().get();
    user = Helenus.dsl(User.class);
  }

  @Test
  public void testPartialInsert() throws Exception {
    Map<String, Object> map = new HashMap<String, Object>();
    Long id = rnd.nextLong();
    map.put("id", id);
    map.put("age", 5);
    InsertOperation<User> insert = session.<User>insert(Helenus.map(User.class, map));
    String cql =
        "INSERT INTO simple_users (id,age) VALUES (" + id.toString() + ",5) IF NOT EXISTS;";
    Assert.assertEquals(cql, insert.cql());
    insert.sync();
  }

  @Test
  public void testPartialUpsert() throws Exception {
    Map<String, Object> map = new HashMap<String, Object>();
    Long id = rnd.nextLong();
    map.put("id", id);
    map.put("age", 5);
    InsertOperation upsert = session.upsert(Helenus.map(User.class, map));
    String cql = "INSERT INTO simple_users (id,age) VALUES (" + id.toString() + ",5);";
    Assert.assertEquals(cql, upsert.cql());
    upsert.sync();
  }
}
