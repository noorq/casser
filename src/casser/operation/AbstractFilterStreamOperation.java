package casser.operation;

import casser.core.Filter;
import casser.dsl.Getter;

public abstract class AbstractFilterStreamOperation<E, O extends AbstractFilterStreamOperation<E, O>> extends AbstractStreamOperation<E, O> {

	public <V> O where(Getter<V> getter, String operator, V val) {
		return (O) this;
	}

	public <V> O where(Filter filter) {
		return (O) this;
	}

	public <V> O and(Getter<V> getter, String operator, V val) {
		return (O) this;
	}

	public <V> O and(Filter filter) {
		return (O) this;
	}

}
