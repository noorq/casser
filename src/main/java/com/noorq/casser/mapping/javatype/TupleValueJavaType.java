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
package com.noorq.casser.mapping.javatype;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.noorq.casser.core.Casser;
import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.IdentityName;
import com.noorq.casser.mapping.MappingUtil;
import com.noorq.casser.mapping.annotation.T;
import com.noorq.casser.mapping.convert.EntityToTupleValueConverter;
import com.noorq.casser.mapping.convert.TupleValueToEntityConverter;
import com.noorq.casser.mapping.convert.TypedConverter;
import com.noorq.casser.mapping.type.AbstractDataType;
import com.noorq.casser.mapping.type.DTDataType;
import com.noorq.casser.support.CasserMappingException;

public final class TupleValueJavaType extends AbstractJavaType {

	@Override
	public Class<?> getJavaClass() {
		return TupleValue.class;
	}

	@Override
	public boolean isApplicable(Class<?> javaClass) {
		return MappingUtil.isTuple(javaClass);
	}
	
	@Override
	public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType) {

		Class<?> javaType = (Class<?>) genericJavaType;

		if (TupleValue.class.isAssignableFrom(javaType)) {
			
			T.Tuple tuple = getter.getDeclaredAnnotation(T.Tuple.class);
			if (tuple == null) {
				throw new CasserMappingException("tuple must be annotated by @Tuple annotation in " + getter);
			}
			
			DataType.Name[] tupleArguments = tuple.value();
			int len = tupleArguments.length;
			DataType[] arguments = new DataType[len];

			for (int i = 0; i != len; ++i) {
				arguments[i] = resolveSimpleType(getter, tupleArguments[i]);
			}
			
			TupleType tupleType = TupleType.of(arguments);
	    	
	    	return new DTDataType(columnType, tupleType, javaType);
			
		}
		else {
			
	    	CasserEntity tupleEntity = Casser.entity(javaType);
	    	
	    	List<DataType> tupleTypes = tupleEntity.getOrderedProperties().stream()
		    	.map(p -> p.getDataType())
		    	.filter(d -> d instanceof DTDataType)
		    	.map(d -> (DTDataType) d)
		    	.map(d -> d.getDataType())
		    	.collect(Collectors.toList());
	    	
	    	if (tupleTypes.size() < tupleEntity.getOrderedProperties().size()) {
	    		
	    		List<IdentityName> wrongColumns = tupleEntity.getOrderedProperties().stream()
	    				.filter(p -> !(p.getDataType() instanceof DTDataType))
	    				.map(p -> p.getColumnName())
	    				.collect(Collectors.toList());
	    		
	    		throw new CasserMappingException("non simple types in tuple " + tupleEntity.getMappingInterface() + " in columns: " + wrongColumns);
	    	}
	    	
	    	TupleType tupleType = TupleType.of(tupleTypes.toArray(new DataType[tupleTypes.size()]));
	    	
	    	return new DTDataType(columnType, tupleType, javaType);
    	
		 }

    	
	}

	@Override
	public Optional<Function<Object, Object>> resolveReadConverter(
			AbstractDataType dataType, SessionRepository repository) {

		DTDataType dt = (DTDataType) dataType;
		
		Class<Object> javaClass = (Class<Object>) dt.getJavaClass();

		if (TupleValue.class.isAssignableFrom(javaClass)) {
			return Optional.empty();
		}

		return Optional.of(TypedConverter.create(
				TupleValue.class,
				javaClass,
				new TupleValueToEntityConverter(javaClass, repository)));
		
	}

	@Override
	public Optional<Function<Object, Object>> resolveWriteConverter(
			AbstractDataType dataType, SessionRepository repository) {

		DTDataType dt = (DTDataType) dataType;
		
		Class<Object> javaClass = (Class<Object>) dt.getJavaClass();
		
		if (TupleValue.class.isAssignableFrom(javaClass)) {
			return Optional.empty();
		}
		
		return Optional.of(TypedConverter.create(
				javaClass, 
				TupleValue.class, 
				new EntityToTupleValueConverter(javaClass, 
						(TupleType) dt.getDataType(), repository)));

	}

}
