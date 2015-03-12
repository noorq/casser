package casser.operation;

import casser.dsl.Getter;

public class DeleteOperation extends AbstractOperation<Object> {

	public <V> DeleteOperation where(Getter<V> getter, String operator, V val) {
		return this;
	}
	
	public <V> DeleteOperation and(Getter<V> getter, String operator, V val) {
		return this;
	}
	
}
