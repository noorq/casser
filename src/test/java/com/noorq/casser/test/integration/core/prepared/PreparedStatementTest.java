package com.noorq.casser.test.integration.core.prepared;

import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BindMarker;
import com.noorq.casser.core.Casser;

import static com.noorq.casser.core.Query.*;

import com.noorq.casser.core.CasserSession;
import com.noorq.casser.core.operation.PreparedOperation;
import com.noorq.casser.test.integration.build.AbstractEmbeddedCassandraTest;

public class PreparedStatementTest extends AbstractEmbeddedCassandraTest {

	Car car;
	
	CasserSession session;
	
	PreparedOperation<ResultSet> insertOp;

	PreparedOperation<ResultSet> updateOp;

	@Before
	public void beforeTest() {

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

	}
	
	
	@Test
	public void test() throws Exception {
		
		insertOp.bind("Nissan", "350Z").sync();
		
		updateOp.bind(Double.valueOf(10000.0), "Nissan", "350Z").sync();
		
		
		
	}
	
}
