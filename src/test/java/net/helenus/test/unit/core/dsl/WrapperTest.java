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
package net.helenus.test.unit.core.dsl;

import java.util.HashMap;
import java.util.Map;
import net.helenus.core.Helenus;
import net.helenus.support.HelenusException;
import org.junit.Assert;
import org.junit.Test;

public class WrapperTest {

  @Test
  public void testWrap() throws Exception {

    Map<String, Object> map = new HashMap<String, Object>();

    map.put("id", 123L);
    map.put("active", Boolean.TRUE);
    map.put("unknownField", "he-he");

    Account account = Helenus.map(Account.class, map);

    Assert.assertEquals(Long.valueOf(123L), account.id());
    Assert.assertTrue(account.active());
  }

  @Test
  public void testPrimitive() throws Exception {

    // NOTE: noramlly a ValueProviderMap for the entity would include all keys for
    // an entity
    // at creation time. This test need to validate that MapperInvocationHander will
    // return
    // the correct default value for an entity, the twist is that if the key doesn't
    // exist
    // in the map then it returns null (so as to support the partial update
    // feature). Here we
    // need to setup the test with a null value for the key we'd like to test.
    Map<String, Object> map = new HashMap<String, Object>();

    map.put("id", 123L);
    map.put("active", null);

    Account account = Helenus.map(Account.class, map);

    Assert.assertFalse(account.active());
  }

  @Test(expected = HelenusException.class)
  public void testWrongMethods() throws Exception {

    WrongAccount wrongAccount = Helenus.map(WrongAccount.class, new HashMap<String, Object>());

    wrongAccount.id();
  }
}
