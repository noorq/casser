package casser.core;

import casser.operation.AbstractStreamOperation;

public class PreparedStreamStatement<E, O extends AbstractStreamOperation<E, O>> {

	public AbstractStreamOperation<E, O> bind(Object... params) {
		return null;
	}
	
}
