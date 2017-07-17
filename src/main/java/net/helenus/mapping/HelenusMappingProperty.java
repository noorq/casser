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
package net.helenus.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Optional;
import java.util.function.Function;

import javax.validation.ConstraintValidator;

import com.datastax.driver.core.Metadata;
import net.helenus.core.SessionRepository;
import net.helenus.mapping.javatype.AbstractJavaType;
import net.helenus.mapping.javatype.MappingJavaTypes;
import net.helenus.mapping.type.AbstractDataType;

public final class HelenusMappingProperty implements HelenusProperty {

	private final HelenusEntity entity;
	private final Method getter;

	private final String propertyName;
	private final Optional<IdentityName> indexName;
	private final boolean caseSensitiveIndex;

	private final ColumnInformation columnInfo;

	private final Type genericJavaType;
	private final Class<?> javaType;
	private final AbstractJavaType abstractJavaType;
	private final AbstractDataType dataType;

	private volatile Optional<Function<Object, Object>> readConverter = null;
	private volatile Optional<Function<Object, Object>> writeConverter = null;

	private final ConstraintValidator<? extends Annotation, ?>[] validators;

	public HelenusMappingProperty(HelenusMappingEntity entity, Method getter, Metadata metadata) {
		this.entity = entity;
		this.getter = getter;

		this.propertyName = MappingUtil.getPropertyName(getter);
		this.indexName = MappingUtil.getIndexName(getter);
		this.caseSensitiveIndex = MappingUtil.caseSensitiveIndex(getter);

		this.columnInfo = new ColumnInformation(getter);

		this.genericJavaType = getter.getGenericReturnType();
		this.javaType = getter.getReturnType();
		this.abstractJavaType = MappingJavaTypes.resolveJavaType(this.javaType);

		this.dataType = abstractJavaType.resolveDataType(this.getter, this.genericJavaType,
				this.columnInfo.getColumnType(), metadata);

		this.validators = MappingUtil.getValidators(getter);
	}

	@Override
	public HelenusEntity getEntity() {
		return entity;
	}

	@Override
	public Class<?> getJavaType() {
		return (Class<?>) javaType;
	}

	@Override
	public AbstractDataType getDataType() {
		return dataType;
	}

	@Override
	public ColumnType getColumnType() {
		return columnInfo.getColumnType();
	}

	@Override
	public int getOrdinal() {
		return columnInfo.getOrdinal();
	}

	@Override
	public OrderingDirection getOrdering() {
		return columnInfo.getOrdering();
	}

	@Override
	public IdentityName getColumnName() {
		return columnInfo.getColumnName();
	}

	@Override
	public Optional<IdentityName> getIndexName() {
		return indexName;
	}

	@Override
	public boolean caseSensitiveIndex() {
		return caseSensitiveIndex;
	}

	@Override
	public String getPropertyName() {
		return propertyName;
	}

	@Override
	public Method getGetterMethod() {
		return getter;
	}

	@Override
	public Optional<Function<Object, Object>> getReadConverter(SessionRepository repository) {

		if (readConverter == null) {
			readConverter = abstractJavaType.resolveReadConverter(this.dataType, repository);
		}

		return readConverter;
	}

	@Override
	public Optional<Function<Object, Object>> getWriteConverter(SessionRepository repository) {

		if (writeConverter == null) {
			writeConverter = abstractJavaType.resolveWriteConverter(this.dataType, repository);
		}

		return writeConverter;
	}

	@Override
	public ConstraintValidator<? extends Annotation, ?>[] getValidators() {
		return validators;
	}

	@Override
	public String toString() {

		StringBuilder str = new StringBuilder();

		String columnName = this.getColumnName().getName();
		str.append("  ");
		str.append(this.getDataType());
		str.append(" ");
		str.append(this.getPropertyName());
		str.append("(");
		if (!columnName.equals(this.getPropertyName())) {
			str.append(columnName);
		}
		str.append(") ");

		ColumnType type = this.getColumnType();

		switch (type) {

			case PARTITION_KEY :
				str.append("partition_key[");
				str.append(this.getOrdinal());
				str.append("] ");
				break;

			case CLUSTERING_COLUMN :
				str.append("clustering_column[");
				str.append(this.getOrdinal());
				str.append("] ");
				OrderingDirection od = this.getOrdering();
				if (od != null) {
					str.append(od.name().toLowerCase()).append(" ");
				}
				break;

			case STATIC_COLUMN :
				str.append("static ");
				break;

			case COLUMN :
				break;

		}

		Optional<IdentityName> idx = this.getIndexName();
		if (idx.isPresent()) {
			str.append("index(").append(idx.get().getName()).append(") ");
		}

		return str.toString();
	}

}
