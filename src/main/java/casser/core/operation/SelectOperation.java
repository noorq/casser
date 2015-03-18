package casser.core.operation;

import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import casser.core.AbstractSessionOperations;
import casser.mapping.CasserMappingEntity;
import casser.mapping.CasserMappingProperty;
import casser.mapping.ColumnValueProvider;
import casser.mapping.RowColumnValueProvider;
import casser.support.CasserMappingException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Select.Selection;


public class SelectOperation<E> extends AbstractFilterStreamOperation<E, SelectOperation<E>> {

	protected final Function<ColumnValueProvider, E> rowMapper;
	
	private final Select select;
	
	public SelectOperation(AbstractSessionOperations sessionOperations, Function<ColumnValueProvider, E> rowMapper, CasserMappingProperty<?>... props) {
		super(sessionOperations);
		
		this.rowMapper = rowMapper;
		
		CasserMappingEntity<?> entity = null;
		Selection selection = QueryBuilder.select();
		
		for (CasserMappingProperty<?> prop : props) {
			selection = selection.column(prop.getColumnName());
			
			if (entity == null) {
				entity = prop.getEntity();
			}
			else if (entity != prop.getEntity()) {
				throw new CasserMappingException("you can select columns only from single entity " + entity.getMappingInterface() + " or " + prop.getEntity().getMappingInterface());
			}
		}
		
		if (entity == null) {
			throw new CasserMappingException("no entity or table to select data");
		}
		
		this.select = selection.from(entity.getTableName());
		
		
	}
	
	public CountOperation count() {
		return null;
	}
	
	public <R> SelectOperation<R> map(Function<E, R> fn) {
		return null;
	}
	
	@Override
	public BuiltStatement getBuiltStatement() {
		return select;
	}

	@Override
	public Stream<E> transform(ResultSet resultSet) {

		return StreamSupport.stream(
				Spliterators.spliteratorUnknownSize(resultSet.iterator(), Spliterator.ORDERED)
				, false).map(r -> new RowColumnValueProvider(r)).map(rowMapper);

	}

	
}
