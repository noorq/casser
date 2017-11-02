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
import org.junit.Assert;
import org.junit.Test;

public class UDTCollectionsDlsTest {

  @Test
  public void testMap() {

    Map<Rocket, String> comments = new HashMap<Rocket, String>();

    Map<String, Object> firstMap = new HashMap<String, Object>();
    firstMap.put("length", 100);
    firstMap.put("price", 100.0);

    Rocket first = Helenus.map(Rocket.class, firstMap);

    Map<String, Object> secondMap = new HashMap<String, Object>();
    secondMap.put("length", 50);
    secondMap.put("price", 70.0);

    Rocket second = Helenus.map(Rocket.class, secondMap);

    Assert.assertEquals(first.hashCode(), first.hashCode());
    Assert.assertEquals(second.hashCode(), second.hashCode());

    Assert.assertFalse(first.equals(second));
    Assert.assertTrue(first.equals(first));
    Assert.assertTrue(second.equals(second));

    comments.put(first, "fast");
    comments.put(second, "nice");

    Assert.assertEquals("fast", comments.get(first));
    Assert.assertEquals("nice", comments.get(second));
  }
}
