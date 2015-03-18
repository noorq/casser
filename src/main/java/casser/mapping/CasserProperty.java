package casser.mapping;

import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.DataType;

public interface CasserProperty<E> {

	CasserEntity<E> getEntity();
	
	String getColumnName();
	
	Class<?> getJavaType();
	
	DataType getDataType();

	boolean isPartitionKey();

	boolean isClusteringColumn();
	
	int getOrdinal();
	
	Ordering getOrdering();
	
	Optional<Function<?, ?>> getReadConverter();
	
	Optional<Function<?, ?>> getWriteConverter();
	
}
