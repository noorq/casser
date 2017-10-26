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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import net.helenus.core.Helenus;
import net.helenus.core.HelenusSession;
import net.helenus.test.integration.build.AbstractEmbeddedCassandraTest;

public class EntityDraftBuilderTest extends AbstractEmbeddedCassandraTest {

	static Supply supply;
	static HelenusSession session;

	@BeforeClass
	public static void beforeTest() {
		session = Helenus.init(getSession()).showCql().add(Supply.class).autoCreateDrop().get();
		supply = session.dsl(Supply.class);
	}

	@Test
	public void testFoo() throws Exception {
		Supply.Draft draft = null;

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
}
