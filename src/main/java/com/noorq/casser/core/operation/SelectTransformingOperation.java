package com.noorq.casser.core.operation;

import java.util.function.Function;
import java.util.stream.Stream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;


public final class SelectTransformingOperation<R, E> extends AbstractFilterStreamOperation<R, SelectTransformingOperation<R, E>> {

	private final SelectOperation<E> src;
	private final Function<E, R> fn;
	
	public SelectTransformingOperation(SelectOperation<E> src, Function<E, R> fn) {
		super(src.sessionOps);
		
		this.src = src;
		this.fn = fn;
		this.filters = src.filters;
		this.ifFilters = src.ifFilters;
	}
	
	@Override
	public BuiltStatement buildStatement() {
		return src.buildStatement();
	}

	@Override
	public Stream<R> transform(ResultSet resultSet) {
		return src.transform(resultSet).map(fn);
	}
	
	
}
