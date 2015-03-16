package casser.core;

import java.io.Closeable;

import casser.core.dsl.Getter;
import casser.core.dsl.Setter;
import casser.core.operation.DeleteOperation;
import casser.core.operation.SelectOperation;
import casser.core.operation.UpdateOperation;
import casser.core.operation.UpsertOperation;
import casser.core.tuple.Tuple1;
import casser.core.tuple.Tuple2;
import casser.core.tuple.Tuple3;

import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Session;

public class CasserSession implements Closeable {

	private Session session;
	
	protected CasserSession(Session session) {
		this.session = session;
	}
	
	public <V1> SelectOperation<Tuple1<V1>> select(Getter<V1> getter1) {
		return null;
	}

	public <V1, V2> SelectOperation<Tuple2<V1, V2>> select(Getter<V1> getter1, Getter<V2> getter2) {
		return null;
	}

	public <V1, V2, V3> SelectOperation<Tuple3<V1, V2, V3>> select(Getter<V1> getter1, Getter<V2> getter2, Getter<V3> getter3) {
		return null;
	}

	public <V1> UpdateOperation update(Setter<V1> setter1, V1 v1) {
		return null;
	}
	
	public UpsertOperation upsert(Object pojo) {
		return null;
	}
	
	public DeleteOperation delete() {
		return null;
	}
	
	public Session getSession() {
		return session;
	}

	public void close() {
		session.close();
	}
	
	public CloseFuture closeAsync() {
		return session.closeAsync();
	}
}
