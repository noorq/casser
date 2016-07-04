/*
 *      Copyright (C) 2015 The Casser Authors
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
package com.noorq.casser.test.integration.core.tuple;



import static com.noorq.casser.core.Query.eq;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class TupleTest extends AbstractEmbeddedCassandraTest {

	static Album album = Casser.dsl(Album.class);

	static CasserSession session;
	
	@BeforeClass
	public static void beforeTest() {
		session = Casser.init(getSession()).showCql().add(album).autoCreateDrop().get();
	}
	
	@Test
	public void testPrint() {
		System.out.println(album);
	}
	
	@Test
	public void testCruid() {
		
		AlbumInformation info = new AlbumInformation() {

			@Override
			public String about() {
				return "Cassandra";
			}

			@Override
			public String place() {
				return "San Jose";
			}
			
		};
		
		
		// CREATE (C)
		
		session.insert()
			.value(album::id, 123)
			.value(album::info, info)
			.sync();
		
		// READ (R)
		
		AlbumInformation actual = session.select(album::info).where(album::id, eq(123)).sync().findFirst().get()._1;
		
		Assert.assertEquals(info.about(), actual.about());
		Assert.assertEquals(info.place(), actual.place());
		
		// UPDATE (U)
		
		// unfortunately this is not working right now in Cassandra, can not update a single column in tuple :(
		//session.update()
		//	.set(album.info()::about, "Casser")
		//	.where(album::id, eq(123))
		//	.sync();
		
		AlbumInformation expected = new AlbumInformation() {

			@Override
			public String about() {
				return "Casser";
			}

			@Override
			public String place() {
				return "Santa Cruz";
			}
			
		};
		
		session.update()
			.set(album::info, expected)
			.where(album::id, eq(123))
			.sync();		
		
		actual = session.select(album::info).where(album::id, eq(123)).sync().findFirst().get()._1;
		
		Assert.assertEquals(expected.about(), actual.about());
		Assert.assertEquals(expected.place(), actual.place());
		
		// INSERT (I) 
		// let's insert null ;)
		
		session.update()
		.set(album::info, null)
		.where(album::id, eq(123))
		.sync();	
		
		actual = session.select(album::info).where(album::id, eq(123)).sync().findFirst().get()._1;
		Assert.assertNull(actual);
		
		// DELETE (D)
		session.delete().where(album::id, eq(123)).sync();

		long cnt = session.select(album::info).where(album::id, eq(123)).sync().count();
		Assert.assertEquals(0, cnt);
		
		
	}
	
	@Test
	public void testNoMapping() {
		
		TupleType tupleType = TupleType.of(DataType.text(), DataType.text());
		TupleValue info = tupleType.newValue();
		
		info.setString(0, "Cassandra");
		info.setString(1, "San Jose");
		
		// CREATE (C)
		
		session.insert()
		.value(album::id, 555)
		.value(album::infoNoMapping, info)
		.sync();
		
		// READ (R)
		
		TupleValue actual = session.select(album::infoNoMapping).where(album::id, eq(555)).sync().findFirst().get()._1;
		
		Assert.assertEquals(info.getString(0), actual.getString(0));
		Assert.assertEquals(info.getString(1), actual.getString(1));
		
		// UPDATE (U)
		
		TupleValue expected = tupleType.newValue();
		
		expected.setString(0, "Casser");
		expected.setString(1, "Los Altos");
		
		session.update()
		.set(album::infoNoMapping, expected)
		.where(album::id, eq(555))
		.sync();	
		
		actual = session.select(album::infoNoMapping).where(album::id, eq(555)).sync().findFirst().get()._1;
		
		Assert.assertEquals(expected.getString(0), actual.getString(0));
		Assert.assertEquals(expected.getString(1), actual.getString(1));
		
		// INSERT (I) 
		// let's insert null ;)
		
		session.update()
		.set(album::infoNoMapping, null)
		.where(album::id, eq(555))
		.sync();	
		
		actual = session.select(album::infoNoMapping).where(album::id, eq(555)).sync().findFirst().get()._1;
		Assert.assertNull(actual);
		
		// DELETE (D)
		session.delete().where(album::id, eq(555)).sync();

		long cnt = session.select(album::infoNoMapping).where(album::id, eq(555)).sync().count();
		Assert.assertEquals(0, cnt);
		
	}
	
}
