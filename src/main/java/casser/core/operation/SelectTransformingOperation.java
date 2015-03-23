package casser.core.operation;

import java.util.function.Function;
import java.util.stream.Stream;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;


public final class SelectTransformingOperation<R, E> extends AbstractFilterStreamOperation<R, SelectTransformingOperation<R, E>> {

	private final SelectOperation<E> src;
	private final Function<E, R> fn;
	
	public SelectTransformingOperation(SelectOperation<E> src, Function<E, R> fn) {
		super(src.sessionOperations);
		
		this.src = src;
		this.fn = fn;
	}
	
	@Override
	public BuiltStatement buildStatement() {
		src.filters = this.filters;
		return src.buildStatement();
	}

	@Override
	public Stream<R> transform(ResultSet resultSet) {
		return src.transform(resultSet).map(fn);
	}
	
	
}
