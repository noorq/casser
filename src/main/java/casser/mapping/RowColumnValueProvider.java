package casser.mapping;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Row;

public class RowColumnValueProvider implements ColumnValueProvider {

	private final Row source;
	private final ColumnDefinitions columnDefinitions;
	
	public RowColumnValueProvider(Row source) {
		this.source = source;
		this.columnDefinitions = source.getColumnDefinitions();
	}
	
	@Override
	public <V> V getColumnValue(int columnIndex, CasserMappingProperty<?> property) {

		if (source.isNull(columnIndex)) {
			return null;
		}
		
		DataType columnType = columnDefinitions.getType(columnIndex);

		if (columnType.isCollection()) {

			List<DataType> typeArguments = columnType.getTypeArguments();

			switch (columnType.getName()) {
			case SET:
				return (V) source.getSet(columnIndex, typeArguments.get(0).asJavaClass());
			case MAP:
				return (V) source.getMap(columnIndex, typeArguments.get(0).asJavaClass(), typeArguments.get(1).asJavaClass());
			case LIST:
				return (V) source.getList(columnIndex, typeArguments.get(0).asJavaClass());
			}

		}

		ByteBuffer bytes = source.getBytesUnsafe(columnIndex);
		Object value = columnType.deserialize(bytes, ProtocolVersion.NEWEST_SUPPORTED);

		if (value != null) {

			Optional<Function<Object, Object>> converter = property.getReadConverter();
			
			if (converter.isPresent()) {
				value = converter.get().apply(value);
			}
			
		}

		return (V) value;
	}

}
