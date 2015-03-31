package com.noorq.casser.core;

import java.util.Objects;

import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.mapping.MappingUtil;
import com.noorq.casser.mapping.OrderingDirection;
import com.noorq.casser.support.CasserMappingException;

public final class Ordered {

	private final Getter<?> getter;
	private final OrderingDirection direction;

	public Ordered(Getter<?> getter, OrderingDirection direction) {
		this.getter = getter;
		this.direction = direction;
	}
	
	public Ordering getOrdering() {
		
		Objects.requireNonNull(getter, "property is null");
		Objects.requireNonNull(direction, "direction is null");
		
		CasserPropertyNode propNode = MappingUtil.resolveMappingProperty(getter);
		
		if (!propNode.getProperty().isClusteringColumn()) {
			throw new CasserMappingException("property must be a clustering column " + propNode.getProperty().getPropertyName());
		}
		
		switch(direction) {
		
			case ASC:
				return QueryBuilder.asc(propNode.getColumnName());
				
			case DESC:
				return QueryBuilder.desc(propNode.getColumnName());
			
		}
		
		throw new CasserMappingException("invalid direction " + direction);
	}
	
}
