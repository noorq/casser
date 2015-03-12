package casser.operation;

import casser.dsl.Getter;

public class DeleteOperation extends AbstractOperation<Object> {

	public <V> DeleteOperation where(Getter<V> getter, V val) {
		return this;
	}
}
