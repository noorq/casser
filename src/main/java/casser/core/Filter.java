package casser.core;

import java.util.Objects;

import casser.core.dsl.Getter;
import casser.mapping.CasserMappingProperty;
import casser.mapping.MappingUtil;
import casser.support.CasserMappingException;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;

public final class Filter<V> {

	private final CasserMappingProperty<?> property;
	private final FilterOperation operation;
	private final V value;
	private final V[] values;
	
	private Filter(CasserMappingProperty<?> prop, FilterOperation op, V value) {
		this.property = prop;
		this.operation = op;
		this.value = value;
		this.values = null;
	}

	private Filter(CasserMappingProperty<?> prop, FilterOperation op, V[] values) {
		this.property = prop;
		this.operation = op;
		this.value = null;
		this.values = values;
	}
	
	public CasserMappingProperty<?> getProperty() {
		return property;
	}

	public FilterOperation getOperation() {
		return operation;
	}

	public V getValue() {
		return value;
	}

	public Clause getClause() {
		
		switch(operation) {
		
		case EQUAL:
			return QueryBuilder.eq(property.getColumnName(), MappingUtil.prepareValueForWrite(property, value));
		
		case IN:
			Object[] preparedValues = new Object[values.length];
			for (int i = 0; i != values.length; ++i) {
				preparedValues[i] = MappingUtil.prepareValueForWrite(property, values[i]);
			}
			return QueryBuilder.in(property.getColumnName(), preparedValues);
			
		case LESSER:
			return QueryBuilder.lt(property.getColumnName(), MappingUtil.prepareValueForWrite(property, value));

		case LESSER_OR_EQUAL:
			return QueryBuilder.lte(property.getColumnName(), MappingUtil.prepareValueForWrite(property, value));

		case GREATER:
			return QueryBuilder.gt(property.getColumnName(), MappingUtil.prepareValueForWrite(property, value));

		case GREATER_OR_EQUAL:
			return QueryBuilder.gte(property.getColumnName(), MappingUtil.prepareValueForWrite(property, value));

		default:
			throw new CasserMappingException("unknown filter operation " + operation);
		}
		
	}
	
	public static <V> Filter<V> equal(Getter<V> getter, V val) {
		return create(getter, FilterOperation.EQUAL, val);
	}

	public static <V> Filter<V> in(Getter<V> getter, V[] vals) {
		Objects.requireNonNull(getter, "empty getter");
		Objects.requireNonNull(vals, "empty values");
		
		if (vals.length == 0) {
			throw new IllegalArgumentException("values array is empty");
		}
		
		CasserMappingProperty<?> prop = MappingUtil.resolveMappingProperty(getter);
		
		return new Filter<V>(prop, FilterOperation.IN, vals);
	}
	
	public static <V> Filter<V> greater(Getter<V> getter, V val) {
		return create(getter, FilterOperation.GREATER, val);
	}
	
	public static <V> Filter<V> less(Getter<V> getter, V val) {
		return create(getter, FilterOperation.LESSER, val);
	}

	public static <V> Filter<V> greaterOrEqual(Getter<V> getter, V val) {
		return create(getter, FilterOperation.GREATER_OR_EQUAL, val);
	}

	public static <V> Filter<V> lessOrEqual(Getter<V> getter, V val) {
		return create(getter, FilterOperation.LESSER_OR_EQUAL, val);
	}

	public static <V> Filter<V> create(Getter<V> getter, String operator, V val) {

		FilterOperation fo = FilterOperation.findByOperator(operator);
		
		if (fo == null) {
			throw new CasserMappingException("invalid operator " + operator);
		}
		
		return create(getter, fo, val);
	}
	
	public static <V> Filter<V> create(Getter<V> getter, FilterOperation op, V val) {
		Objects.requireNonNull(getter, "empty getter");
		Objects.requireNonNull(val, "empty value");
		
		if (op == FilterOperation.IN) {
			throw new IllegalArgumentException("invalid usage of the 'in' operator, use Filter.in() static method");
		}
		
		CasserMappingProperty<?> prop = MappingUtil.resolveMappingProperty(getter);
		
		return new Filter<V>(prop, op, val);
	}
	
}
