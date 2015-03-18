package casser.mapping;

public interface ColumnValueProvider {

	<V> V getColumnValue(int columnIndex, CasserMappingProperty<?> property);
	
}
