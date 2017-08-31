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

import com.datastax.driver.core.utils.UUIDs;
import net.bytebuddy.utility.RandomString;
import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.core.UnitOfWork;
import net.helenus.core.annotation.Cacheable;
import net.helenus.mapping.annotation.Column;
import net.helenus.mapping.annotation.PartitionKey;
import net.helenus.mapping.annotation.Table;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.UUID;

import static net.helenus.core.Query.eq;


@Table
@Cacheable
interface Widget {
    @PartitionKey
    UUID id();
    @Column
    String name();
}


public class UnitOfWorkTest extends AbstractEmbeddedCassandraTest {

    static Widget widget;
    static HelenusSession session;


    @BeforeClass
    public static void beforeTest() {
        session = Helenus.init(getSession())
                .showCql()
                .add(Widget.class)
                .autoCreateDrop()
                .get();
        widget = session.dsl(Widget.class);
    }

    @Test
    public void testSelectAfterInsertProperlyCachesEntity() throws Exception {
        /*
        Widget w1, w2, w3, w4;
        UUID key = UUIDs.timeBased();

        try (UnitOfWork uow = session.begin()) {

            // This should cache the inserted Widget.
            w1 = session.<Widget>upsert(widget)
                    .value(widget::id, key)
                    .value(widget::name, RandomString.make(20))
                    .sync(uow);

            // This should read from the cache and get the same instance of a Widget.
            w2 = session.<Widget>select(widget)
                    .where(widget::id, eq(key))
                    .single()
                    .sync(uow)
                    .orElse(null);

            uow.commit()
                    .andThen(() -> {
                        Assert.assertEquals(w1, w2);
                    });
        }

        // This should read the widget from the session cache and maintain object identity.
        w3 = session.<Widget>select(widget)
                .where(widget::id, eq(key))
                .single()
                .sync()
                .orElse(null);

        Assert.assertEquals(w1, w3);

        // This should read the widget from the database, no object identity but values should match.
        w4 = session.<Widget>select(widget)
                .where(widget::id, eq(key))
                .ignoreCache()
                .single()
                .sync()
                .orElse(null);

        Assert.assertNotEquals(w1, w4);
        Assert.assertTrue(w1.equals(w4));
        */
    }

}
