package casser.mapping;

import java.util.Collection;

public interface CasserEntity<E> {

	String getTableName();
	
	Collection<CasserProperty<E>> getProperties();
	
}
