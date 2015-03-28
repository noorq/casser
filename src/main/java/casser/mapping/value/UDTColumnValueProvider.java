/*
 *      Copyright (C) 2015 Noorq, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package casser.mapping.value;

import java.nio.ByteBuffer;
import java.util.Optional;
import java.util.function.Function;

import casser.mapping.CasserMappingProperty;
import casser.mapping.CasserMappingRepository;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;

public final class UDTColumnValueProvider implements ColumnValueProvider {

	private final CasserMappingRepository repository;
	
	public UDTColumnValueProvider(CasserMappingRepository repository) {
		this.repository = repository;
	}
	
	@Override
	public <V> V getColumnValue(Object sourceObj, int columnIndexUnused, CasserMappingProperty property) {

		UDTValue source = (UDTValue) sourceObj;
		
		UserType userType = source.getType();
		
		String name = property.getColumnName();
		
		DataType fieldType = userType.getFieldType(name);
		
		ByteBuffer bytes = source.getBytesUnsafe(name);
		Object value = fieldType.deserialize(bytes, ProtocolVersion.NEWEST_SUPPORTED);
		
		if (value != null) {

			Optional<Function<Object, Object>> converter = property.getReadConverter(repository);
			
			if (converter.isPresent()) {
				value = converter.get().apply(value);
			}
			
		}
		
		return (V) value;
		
	}
}
