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

import static net.helenus.core.Query.eq;

import java.util.UUID;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.utils.UUIDs;

import net.bytebuddy.utility.RandomString;
import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.core.UnitOfWork;
import net.helenus.core.annotation.Cacheable;
import net.helenus.mapping.annotation.Constraints;
import net.helenus.mapping.annotation.Index;
import net.helenus.mapping.annotation.PartitionKey;
import net.helenus.mapping.annotation.Table;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;

@Table
@Cacheable
interface Widget {
	@PartitionKey
	UUID id();

	@Index
	@Constraints.Distinct()
	String name();
}

public class UnitOfWorkTest extends AbstractEmbeddedCassandraTest {

	static Widget widget;
	static HelenusSession session;

	@BeforeClass
	public static void beforeTest() {
		session = Helenus.init(getSession()).showCql().add(Widget.class).autoCreateDrop().get();
		widget = session.dsl(Widget.class);
	}

	@Test
	public void testSelectAfterSelect() throws Exception {
		Widget w1, w2, w3, w4;
		UUID key = UUIDs.timeBased();

		// This should inserted Widget, but not cache it.
		w1 = session.<Widget>insert(widget).value(widget::id, key).value(widget::name, RandomString.make(20)).sync();

		try (UnitOfWork uow = session.begin()) {

			uow.setPurpose("testSelectAfterSelect");

			// This should read from the database and return a Widget.
			w2 = session.<Widget>select(widget).where(widget::id, eq(key)).single().sync(uow).orElse(null);

			// This should read from the cache and get the same instance of a Widget.
			w3 = session.<Widget>select(widget).where(widget::id, eq(key)).single().sync(uow).orElse(null);

			uow.commit().andThen(() -> {
				Assert.assertEquals(w2, w3);
			});
		}

		w4 = session.<Widget>select(widget).where(widget::name, eq(w1.name())).single().sync().orElse(null);
		Assert.assertEquals(w4, w1);
	}

	@Test
	public void testSelectAfterNestedSelect() throws Exception {
		Widget w1, w2, w3, w4;
		UUID key1 = UUIDs.timeBased();
		UUID key2 = UUIDs.timeBased();

		// This should inserted Widget, and not cache it in uow1.
		try (UnitOfWork uow1 = session.begin()) {
			w1 = session.<Widget>insert(widget).value(widget::id, key1).value(widget::name, RandomString.make(20))
					.sync(uow1);

			try (UnitOfWork uow2 = session.begin(uow1)) {

				// This should read from uow1's cache and return the same Widget.
				w2 = session.<Widget>select(widget).where(widget::id, eq(key1)).single().sync(uow2).orElse(null);

				Assert.assertEquals(w1, w2);

				w3 = session.<Widget>insert(widget).value(widget::id, key2).value(widget::name, RandomString.make(20))
						.sync(uow2);

				uow2.commit().andThen(() -> {
					Assert.assertEquals(w1, w2);
				});
			}

			// This should read from the cache and get the same instance of a Widget.
			w4 = session.<Widget>select(widget).where(widget::id, eq(key2)).single().sync(uow1).orElse(null);

			uow1.commit().andThen(() -> {
				Assert.assertEquals(w3, w4);
			});
		}
	}

	@Test
	public void testSelectViaIndexAfterSelect() throws Exception {
		Widget w1, w2;
		UUID key = UUIDs.timeBased();

		try (UnitOfWork uow = session.begin()) {
			// This should insert and cache Widget in the uow.
			session.<Widget>insert(widget).value(widget::id, key).value(widget::name, RandomString.make(20)).sync(uow);

			// This should read from the database and return a Widget.
			w1 = session.<Widget>select(widget).where(widget::id, eq(key)).single().sync(uow).orElse(null);

			// This should read from the cache and get the same instance of a Widget.
			w2 = session.<Widget>select(widget).where(widget::name, eq(w1.name())).single().sync(uow).orElse(null);

			uow.commit().andThen(() -> {
				Assert.assertEquals(w1, w2);
			});
		}
	}

    @Test
    public void testSelectAfterUpdated() throws Exception {
        Widget w1, w2, w3, w4, w5, w6;
        UUID key = UUIDs.timeBased();

        // This should inserted Widget, but not cache it.
        w1 = session.<Widget>insert(widget).value(widget::id, key).value(widget::name, RandomString.make(20)).sync();

        try (UnitOfWork uow = session.begin()) {

            // This should read from the database and return a Widget.
            w2 = session.<Widget>select(widget).where(widget::id, eq(key)).single()
                    .sync(uow).orElse(null);
            Assert.assertEquals(w1, w2);

            // This should remove the object from the session cache.
            w3 = session.<Widget>update(w2)
                    .set(widget::name, "Bill")
                    .where(widget::id, eq(key))
                    .sync(uow);

            // Fetch from session cache, should have old name.
            w4 = session.<Widget>select(widget).where(widget::id, eq(key)).single()
                    .sync().orElse(null);
            Assert.assertEquals(w4, w2);
            Assert.assertEquals(w4.name(), w1.name());

            // This should skip the cache.
            w5 = session.<Widget>select(widget).where(widget::id, eq(key)).single()
                    .uncached()
                    .sync().orElse(null);

            Assert.assertNotEquals(w5, w2); // Not the same instance,
            Assert.assertTrue(w2.equals(w5)); // but they have the same values,
            Assert.assertFalse(w5.equals(w2)); // regardless of the order when comparing.
            Assert.assertEquals(w5.name(), "Bill");

            uow.commit().andThen(() -> {
                Assert.assertEquals(w1, w2);
            });
        }

        // The name changed, this should miss cache and not find anything in the database.
        w6 = session.<Widget>select(widget).where(widget::name, eq(w1.name())).single()
                .sync().orElse(null);
        Assert.assertTrue(w2.equals(w5));
    }


	@Test
	public void testSelectAfterDeleted() throws Exception {
		Widget w1, w2, w3, w4;
		UUID key = UUIDs.timeBased();

		// This should inserted Widget, but not cache it.
		w1 = session.<Widget>insert(widget).value(widget::id, key).value(widget::name, RandomString.make(20)).sync();

		try (UnitOfWork uow = session.begin()) {

            // This should read from the database and return a Widget.
            w2 = session.<Widget>select(widget).where(widget::id, eq(key)).single()
                    .sync(uow).orElse(null);

            // This should remove the object from the cache.
            session.delete(widget).where(widget::id, eq(key))
                    .sync(uow);

            // This should fail to read from the cache.
            w3 = session.<Widget>select(widget).where(widget::id, eq(key)).single()
                    .sync(uow).orElse(null);

            Assert.assertEquals(w3, null);

            uow.commit().andThen(() -> {
                Assert.assertEquals(w1, w2);
                Assert.assertEquals(w3, null);
            });
        }

        w4 = session.<Widget>select(widget).where(widget::name, eq(w1.name())).single()
                .sync().orElse(null);

        Assert.assertEquals(w4, null);
    }
/*
	@Test
	public void testInsertNoOp() throws Exception {
		Widget w1, w2;
		UUID key = UUIDs.timeBased();


		try (UnitOfWork uow = session.begin()) {
			// This should inserted Widget, but not cache it.
			w1 = session.<Widget>insert(widget).value(widget::id, key).value(widget::name, RandomString.make(20)).sync(uow);
			w2 = session.<Widget>insert(w1).value(widget::id, key).sync(uow);
		}
		Assert.assertEquals(w1, w2);
	}
*/
	/*
	 * @Test public void testSelectAfterInsertProperlyCachesEntity() throws
	 * Exception { Widget w1, w2, w3, w4; UUID key = UUIDs.timeBased();
	 * 
	 * try (UnitOfWork uow = session.begin()) {
	 * 
	 * // This should cache the inserted Widget. w1 = session.<Widget>insert(widget)
	 * .value(widget::id, key) .value(widget::name, RandomString.make(20))
	 * .sync(uow);
	 * 
	 * // This should read from the cache and get the same instance of a Widget. w2
	 * = session.<Widget>select(widget) .where(widget::id, eq(key)) .single()
	 * .sync(uow) .orElse(null);
	 * 
	 * uow.commit() .andThen(() -> { Assert.assertEquals(w1, w2); }); }
	 * 
	 * // This should read the widget from the session cache and maintain object
	 * identity. w3 = session.<Widget>select(widget) .where(widget::id, eq(key))
	 * .single() .sync() .orElse(null);
	 * 
	 * Assert.assertEquals(w1, w3);
	 * 
	 * // This should read the widget from the database, no object identity but
	 * values should match. w4 = session.<Widget>select(widget) .where(widget::id,
	 * eq(key)) .uncached() .single() .sync() .orElse(null);
	 * 
	 * Assert.assertNotEquals(w1, w4); Assert.assertTrue(w1.equals(w4)); }
	 */
}
