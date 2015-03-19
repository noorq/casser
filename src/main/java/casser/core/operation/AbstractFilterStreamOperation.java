package casser.core.operation;

import java.util.LinkedList;
import java.util.List;

import casser.core.AbstractSessionOperations;
import casser.core.Filter;
import casser.core.dsl.Getter;

public abstract class AbstractFilterStreamOperation<E, O extends AbstractFilterStreamOperation<E, O>> extends AbstractStreamOperation<E, O> {

	protected List<Filter<?>> filters = null;
	
	public AbstractFilterStreamOperation(AbstractSessionOperations sessionOperations) {
		super(sessionOperations);
	}
	
	public <V> O where(Getter<V> getter, String operator, V val) {
		
		addFilter(Filter.create(getter, operator, val));
		
		return (O) this;
	}

	public <V> O where(Filter<V> filter) {

		addFilter(filter);

		return (O) this;
	}

	public <V> O and(Getter<V> getter, String operator, V val) {
		
		addFilter(Filter.create(getter, operator, val));
		
		return (O) this;
	}

	public <V> O and(Filter<V> filter) {
		
		addFilter(filter);
		
		return (O) this;
	}
	
	private void addFilter(Filter<?> filter) {
		if (filters == null) {
			filters = new LinkedList<Filter<?>>();
		}
		filters.add(filter);
	}


}
