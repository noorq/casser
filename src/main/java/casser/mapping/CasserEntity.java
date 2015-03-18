package casser.mapping;

import java.util.Collection;

public interface CasserEntity<E> {

	String getName();
	
	String getTableName();
	
	Collection<CasserProperty<E>> getProperties();
	
}
