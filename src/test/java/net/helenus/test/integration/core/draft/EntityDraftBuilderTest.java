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
package net.helenus.test.integration.core.draft;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.concurrent.TimeoutException;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;

import static net.helenus.core.Query.eq;

public class EntityDraftBuilderTest extends AbstractEmbeddedCassandraTest {

	static Supply supply;
	static HelenusSession session;
    static Supply.Draft draft = null;

	@BeforeClass
	public static void beforeTest() throws TimeoutException {
		session = Helenus.init(getSession()).showCql().add(Supply.class).autoCreateDrop().get();
        supply = session.dsl(Supply.class);

        draft = Supply.draft("APAC").code("WIDGET-002").description("Our second Widget!")
                .demand(new HashMap<String, Long>() {
                    {
                        put("APAC", 100L);
                        put("EMEA", 10000L);
                        put("NORAM", 2000000L);
                    }
                }).shipments(new HashSet<String>() {
                    {
                        add("HMS Puddle in transit to APAC, 100 units.");
                        add("Frigate Jimmy in transit to EMEA, 10000 units.");
                    }
                }).suppliers(new ArrayList<String>() {
                    {
                        add("Puddle, Inc.");
                        add("Jimmy Town, LTD.");
                    }
                });

        Supply s1 = session.<Supply>insert(draft).sync();
    }

	@Test
	public void testFoo() throws Exception {

        Supply s1 = session.<Supply>select(Supply.class).where(supply::id, eq(draft.id()))
                .single()
                .sync()
                .orElse(null);

		// List
		Supply s2 = session.<Supply>update(s1.update()).prepend(supply::suppliers, "Pignose Supply, LLC.").sync();
		Assert.assertEquals(s2.suppliers().get(0), "Pignose Supply, LLC.");

		// Set
		String shipment = "Pignose, on the way! (1M units)";
		Supply s3 = session.<Supply>update(s2.update()).add(supply::shipments, shipment).sync();
		Assert.assertTrue(s3.shipments().contains(shipment));

		// Map
		Supply s4 = session.<Supply>update(s3.update()).put(supply::demand, "NORAM", 10L).sync();
		Assert.assertEquals((long) s4.demand().get("NORAM"), 10L);
	}

	@Test
    public void testSerialization() throws Exception {
        Supply s1, s2;

        s1 = session.<Supply>select(Supply.class).where(supply::id, eq(draft.id()))
                .single()
                .sync()
                .orElse(null);

        byte[] data;
        try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(bos)) {
                out.writeObject(s1);
                out.flush();
                data = bos.toByteArray();
        }

        try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
            ObjectInput in = new ObjectInputStream(bis)) {
                s2 = (Supply)in.readObject();
        }

        Assert.assertEquals(s2.id(), s1.id());
        Assert.assertEquals(s2, s1);
    }
}
