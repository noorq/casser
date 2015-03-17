package casser.mapping;

public interface CasserProperty<E> {

	CasserEntity<E> getEntity();
	
	String getColumnName();

	boolean isPrimaryKey();
	
	KeyType getKeyType();
	
	int getOrdinal();
	
	Ordering getOrdering();
	
}
