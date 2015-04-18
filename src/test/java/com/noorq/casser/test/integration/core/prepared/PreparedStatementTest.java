/*
 *      Copyright (C) 2015 Noorq, Inc.
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
package com.noorq.casser.test.integration.core.prepared;

import static com.noorq.casser.core.Query.eq;
import static com.noorq.casser.core.Query.marker;

import java.math.BigDecimal;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.CasserSession;
import com.noorq.casser.core.operation.PreparedOperation;
import com.noorq.casser.core.operation.PreparedStreamOperation;
import com.noorq.casser.support.Fun;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class PreparedStatementTest extends AbstractEmbeddedCassandraTest {

	static Car car;
	
	static CasserSession session;
	
	static PreparedOperation<ResultSet> insertOp;

	static PreparedOperation<ResultSet> updateOp;

	static PreparedStreamOperation<Car> selectOp;
	
	static PreparedStreamOperation<Fun.Tuple1<BigDecimal>> selectPriceOp;
	
	static PreparedOperation<ResultSet> deleteOp;
	
	static PreparedOperation<Long> countOp;

	
	@BeforeClass
	public static void beforeTest() {

		car = Casser.dsl(Car.class);
		
		session = Casser.init(getSession()).showCql().add(Car.class).autoCreateDrop().get();
		
		insertOp = session.insert()
		  .value(car::make, marker())
		  .value(car::model, marker())
		  .value(car::year, 2004)
		  .prepare();
		
		updateOp = session.update()
				.set(car::price, marker())
				.where(car::make, eq(marker()))
				.and(car::model, eq(marker()))
				.prepare();

		selectOp = session.select(Car.class)
				.where(car::make, eq(marker()))
				.and(car::model, eq(marker()))
				.prepare();
		
		selectPriceOp = session.select(car::price)
				.where(car::make, eq(marker()))
				.and(car::model, eq(marker()))
				.prepare();
		
		deleteOp = session.delete()
				.where(car::make, eq(marker()))
				.and(car::model, eq(marker()))
				.prepare();
		
		countOp = session.count()
				.where(car::make, eq(marker()))
				.and(car::model, eq(marker()))
				.prepare();
		
	}
	
	@Test
	public void testPrint() {
		System.out.println(car);
	}
	
	@Test
	public void testCRUID() throws Exception {
		
		// INSERT
		
		insertOp.bind("Nissan", "350Z").sync();
		
		// SELECT
		
		Car actual = selectOp.bind("Nissan", "350Z").sync().findFirst().get();
		Assert.assertEquals("Nissan", actual.make());
		Assert.assertEquals("350Z", actual.model());
		Assert.assertEquals(2004, actual.year());
		Assert.assertNull(actual.price());

		// UPDATE
		
		updateOp.bind(BigDecimal.valueOf(10000.0), "Nissan", "350Z").sync();

		BigDecimal price = selectPriceOp.bind("Nissan", "350Z").sync().findFirst().get()._1;
		
		Assert.assertEquals(BigDecimal.valueOf(10000.0), price);
		
		// DELETE
		
		Long cnt = countOp.bind("Nissan", "350Z").sync();
		Assert.assertEquals(Long.valueOf(1), cnt);
		
		deleteOp.bind("Nissan", "350Z").sync();
		
		cnt = countOp.bind("Nissan", "350Z").sync();
		Assert.assertEquals(Long.valueOf(0), cnt);
		
	}
	
}
