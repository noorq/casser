package casser.operation;

import casser.dsl.Getter;

public class UpdateOperation extends AbstractOperation<Object> {

	public <V> UpdateOperation where(Getter<V> getter, String operator, V val) {
		return this;
	}

	public <V> UpdateOperation and(Getter<V> getter, String operator, V val) {
		return this;
	}
	
}
