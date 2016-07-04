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

import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class InnerTupleTest extends AbstractEmbeddedCassandraTest {

	static PhotoAlbum photoAlbum = Casser.dsl(PhotoAlbum.class);

	static CasserSession session;
	
	@BeforeClass
	public static void beforeTest() {
		session = Casser.init(getSession()).showCql().add(photoAlbum).autoCreateDrop().get();
	}
	
	@Test
	public void testPrint() {
		System.out.println(photoAlbum);
	}
	
	
	@Test
	public void testCruid() {
		
		Photo photo = new Photo() {

			@Override
			public byte[] blob() {
				return "jpeg".getBytes();
			}
			
		};
		
		PhotoFolder folder = new PhotoFolder() {

			@Override
			public String name() {
				return "first";
			}

			@Override
			public Photo photo() {
				return photo;
			}
			
		};
		
		// CREATE (C)
		
		session.insert()
			.value(photoAlbum::id, 123)
			.value(photoAlbum::folder, folder)
			.sync();
		
		// READ (R)
		
		PhotoFolder actual = session.select(photoAlbum::folder).where(photoAlbum::id, eq(123)).sync().findFirst().get()._1;
		
		Assert.assertEquals(folder.name(), actual.name());
		
		// UPDATE (U)
		
		// unfortunately this is not working right now in Cassandra, can not update a single column in tuple :(
		//session.update()
		//	.set(photoAlbum.folder().photo()::blob, "Casser".getBytes())
		//	.where(photoAlbum::id, eq(123))
		//	.sync();
		
		PhotoFolder expected = new PhotoFolder() {

			@Override
			public String name() {
				return "seconds";
			}

			@Override
			public Photo photo() {
				return photo;
			}
			
		};
		
		session.update()
			.set(photoAlbum::folder, expected)
			.where(photoAlbum::id, eq(123))
			.sync();		
		
		actual = session.select(photoAlbum::folder).where(photoAlbum::id, eq(123)).sync().findFirst().get()._1;
		
		Assert.assertEquals(expected.name(), actual.name());
		
		// INSERT (I) 
		// let's insert null ;)
		
		session.update()
		.set(photoAlbum::folder, null)
		.where(photoAlbum::id, eq(123))
		.sync();	
		
		actual = session.select(photoAlbum::folder).where(photoAlbum::id, eq(123)).sync().findFirst().get()._1;
		Assert.assertNull(actual);
		
		// DELETE (D)
		session.delete().where(photoAlbum::id, eq(123)).sync();

		long cnt = session.select(photoAlbum::folder).where(photoAlbum::id, eq(123)).sync().count();
		Assert.assertEquals(0, cnt);
		
		
	}
	
	
}
