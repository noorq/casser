package casser.mapping;

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
	
}
