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
package com.noorq.casser.core.reflect;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;

import javax.validation.ConstraintValidator;

import com.noorq.casser.core.SessionRepository;
import com.noorq.casser.mapping.CasserEntity;
import com.noorq.casser.mapping.CasserProperty;
import com.noorq.casser.mapping.ColumnType;
import com.noorq.casser.mapping.IdentityName;
import com.noorq.casser.mapping.MappingUtil;
import com.noorq.casser.mapping.OrderingDirection;
import com.noorq.casser.mapping.type.AbstractDataType;
import com.noorq.casser.support.CasserMappingException;

public final class CasserNamedProperty implements CasserProperty {

	private final String name;
	
	public CasserNamedProperty(String name) {
		this.name = name;
	}

	@Override
	public CasserEntity getEntity() {
		throw new CasserMappingException("will never called");
	}

	@Override
	public String getPropertyName() {
		return name;
	}

	@Override
	public Method getGetterMethod() {
		throw new CasserMappingException("will never called");
	}

	@Override
	public IdentityName getColumnName() {
		return IdentityName.of(name, false);
	}

	@Override
	public Optional<IdentityName> getIndexName() {
		return Optional.empty();
	}

	@Override
	public Class<?> getJavaType() {
		throw new CasserMappingException("will never called");
	}

	@Override
	public AbstractDataType getDataType() {
		throw new CasserMappingException("will never called");
	}

	@Override
	public ColumnType getColumnType() {
		return ColumnType.COLUMN;
	}

	@Override
	public int getOrdinal() {
		return 0;
	}

	@Override
	public OrderingDirection getOrdering() {
		return OrderingDirection.ASC;
	}

	@Override
	public Optional<Function<Object, Object>> getReadConverter(
			SessionRepository repository) {
		return Optional.empty();
	}

	@Override
	public Optional<Function<Object, Object>> getWriteConverter(
			SessionRepository repository) {
		return Optional.empty();
	}
	
	@Override
	public ConstraintValidator<? extends Annotation, ?>[] getValidators() {
		return MappingUtil.EMPTY_VALIDATORS;
	}
}
