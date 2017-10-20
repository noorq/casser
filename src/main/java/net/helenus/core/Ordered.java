package net.helenus.core;

import java.util.Objects;

import com.datastax.driver.core.querybuilder.Ordering;
import com.datastax.driver.core.querybuilder.QueryBuilder;

import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.ColumnType;
import net.helenus.mapping.MappingUtil;
import net.helenus.mapping.OrderingDirection;
import net.helenus.support.HelenusMappingException;

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

		HelenusPropertyNode propNode = MappingUtil.resolveMappingProperty(getter);

		if (propNode.getProperty().getColumnType() != ColumnType.CLUSTERING_COLUMN) {
			throw new HelenusMappingException(
					"property must be a clustering column " + propNode.getProperty().getPropertyName());
		}

		switch (direction) {
			case ASC :
				return QueryBuilder.asc(propNode.getColumnName());

			case DESC :
				return QueryBuilder.desc(propNode.getColumnName());
		}

		throw new HelenusMappingException("invalid direction " + direction);
	}
}
