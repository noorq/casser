package casser.operation;

import casser.dsl.Getter;

public class UpdateOperation extends AbstractOperation<Object> {

	public <V> UpdateOperation where(Getter<V> getter, V val) {
		return this;
	}

	
}
