/*
 *      Copyright (C) 2015 The Casser Authors
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
package com.noorq.casser.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;

import javax.validation.ConstraintValidator;

import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.type.AbstractDataType;

public interface CasserProperty {

	CasserEntity getEntity();

	String getPropertyName(); 
	
	Method getGetterMethod();
	
	IdentityName getColumnName();
	
	Optional<IdentityName> getIndexName();
	
	Class<?> getJavaType();
	
	AbstractDataType getDataType();
	
	ColumnType getColumnType();
	
	int getOrdinal();
	
	OrderingDirection getOrdering();
	
	Optional<Function<Object, Object>> getReadConverter(SessionRepository repository);
	
	Optional<Function<Object, Object>> getWriteConverter(SessionRepository repository);

	ConstraintValidator<? extends Annotation, ?>[] getValidators();
	
}
