/*
 *      Copyright (C) 2015 The Helenus Authors
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
package net.helenus.core.reflect;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.Function;

import javax.validation.ConstraintValidator;

import net.helenus.core.SessionRepository;
import net.helenus.mapping.*;
import net.helenus.mapping.type.AbstractDataType;
import net.helenus.support.HelenusMappingException;

public final class HelenusNamedProperty implements HelenusProperty {

	private final String name;

	public HelenusNamedProperty(String name) {
		this.name = name;
	}

	@Override
	public HelenusEntity getEntity() {
		throw new HelenusMappingException("will never called");
	}

	@Override
	public String getPropertyName() {
		return name;
	}

	@Override
	public Method getGetterMethod() {
		throw new HelenusMappingException("will never called");
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
	public boolean caseSensitiveIndex() {
		return false;
	}

	@Override
	public Class<?> getJavaType() {
		throw new HelenusMappingException("will never called");
	}

	@Override
	public AbstractDataType getDataType() {
		throw new HelenusMappingException("will never called");
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
	public Optional<Function<Object, Object>> getReadConverter(SessionRepository repository) {
		return Optional.empty();
	}

	@Override
	public Optional<Function<Object, Object>> getWriteConverter(SessionRepository repository) {
		return Optional.empty();
	}

	@Override
	public ConstraintValidator<? extends Annotation, ?>[] getValidators() {
		return MappingUtil.EMPTY_VALIDATORS;
	}
}
