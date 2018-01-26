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
package net.helenus.test.integration.core.unitofwork;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.core.UnitOfWork;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class AndThenOrderTest extends AbstractEmbeddedCassandraTest {

  static HelenusSession session;

  @BeforeClass
  public static void beforeTest() {
    session = Helenus.init(getSession()).showCql().autoCreateDrop().get();
  }

  @Test
  public void testAndThenOrdering() throws Exception {
    List<String> q = new ArrayList<String>(5);
    UnitOfWork uow1, uow2, uow3, uow4, uow5;

    uow5 = session.begin();
    uow3 = session.begin(uow5);
    uow1 = session.begin(uow3);
    uow1.commit()
        .andThen(
            () -> {
              q.add("1");
            });
    uow2 = session.begin(uow3);
    uow2.commit()
        .andThen(
            () -> {
              q.add("2");
            });
    uow3.commit()
        .andThen(
            () -> {
              q.add("3");
            });
    uow4 = session.begin(uow5);
    uow4.commit()
        .andThen(
            () -> {
              q.add("4");
            });
    uow5.commit()
        .andThen(
            () -> {
              q.add("5");
            });

    System.out.println(q);
    Assert.assertTrue(
        Arrays.equals(q.toArray(new String[5]), new String[] {"1", "2", "3", "4", "5"}));
  }

  @Test
  public void testExceptionWithinAndThen() throws Exception {
    List<String> q = new ArrayList<String>(5);
    UnitOfWork uow1, uow2, uow3, uow4, uow5;

    uow5 = session.begin();
    uow4 = session.begin(uow5);
    try {
      uow3 = session.begin(uow4);
      uow1 = session.begin(uow3);
      uow1.commit()
          .andThen(
              () -> {
                q.add("1");
              })
          .exceptionally(
              () -> {
                q.add("a");
              });
      uow2 = session.begin(uow3);
      uow2.commit()
          .andThen(
              () -> {
                q.add("2");
              })
          .exceptionally(
              () -> {
                q.add("b");
              });
      uow3.commit()
          .andThen(
              () -> {
                q.add("3");
              })
          .exceptionally(
              () -> {
                q.add("c");
              });
      uow4.commit()
          .andThen(
              () -> {
                q.add("4");
              })
          .exceptionally(
              () -> {
                q.add("d");
              });
      throw new Exception();
    } catch (Exception e) {
      uow4.abort();
    }
    uow5.commit()
        .andThen(
            () -> {
              q.add("5");
            })
        .exceptionally(
            () -> {
              q.add("e");
            });

    System.out.println(q);
    Assert.assertTrue(
        Arrays.equals(q.toArray(new String[5]), new String[] {"a", "b", "c", "d", "e"}));
  }

  @Test
  public void testClosableWillAbortWhenNotCommitted() throws Exception {
    UnitOfWork unitOfWork;
    try (UnitOfWork uow = session.begin()) {
      unitOfWork = uow;
      Assert.assertFalse(uow.hasAborted());
    }
    Assert.assertTrue(unitOfWork.hasAborted());
  }

  @Test
  public void testClosable() throws Exception {
    UnitOfWork unitOfWork;
    try (UnitOfWork uow = session.begin()) {
      unitOfWork = uow;
      Assert.assertFalse(uow.hasAborted());
      uow.commit()
          .andThen(
              () -> {
                Assert.assertFalse(uow.hasAborted());
                Assert.assertTrue(uow.hasCommitted());
              });
    }
    Assert.assertFalse(unitOfWork.hasAborted());
    Assert.assertTrue(unitOfWork.hasCommitted());
  }
}
