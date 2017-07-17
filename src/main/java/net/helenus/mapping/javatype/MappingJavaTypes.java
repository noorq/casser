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
package net.helenus.mapping.javatype;

import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.util.Optional;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.google.common.collect.ImmutableMap;

import net.helenus.mapping.ColumnType;
import net.helenus.mapping.type.AbstractDataType;
import net.helenus.mapping.type.DTDataType;
import net.helenus.support.HelenusMappingException;

public final class MappingJavaTypes {

	private static final EnumJavaType ENUM_JAVA_TYPE = new EnumJavaType();
	private static final UDTValueJavaType UDT_VALUE_JAVA_TYPE = new UDTValueJavaType();
	private static final TupleValueJavaType TUPLE_VALUE_JAVA_TYPE = new TupleValueJavaType();

	private static final ImmutableMap<Class<?>, AbstractJavaType> knownTypes;

	static {

		ImmutableMap.Builder<Class<?>, AbstractJavaType> builder = ImmutableMap.builder();

		add(builder, new BooleanJavaType());
		add(builder, new BigDecimalJavaType());
		add(builder, new BigIntegerJavaType());
		add(builder, new DoubleJavaType());
		add(builder, new FloatJavaType());
		add(builder, new IntegerJavaType());
		add(builder, new InetAddressJavaType());

		add(builder, new ByteBufferJavaType());
		add(builder, new ByteArrayJavaType());
		add(builder, new DateJavaType());
		add(builder, new UUIDJavaType());
		add(builder, new LongJavaType());
		add(builder, new StringJavaType());
		add(builder, ENUM_JAVA_TYPE);
		add(builder, new ListJavaType());
		add(builder, new SetJavaType());
		add(builder, new MapJavaType());
		add(builder, TUPLE_VALUE_JAVA_TYPE);
		add(builder, UDT_VALUE_JAVA_TYPE);

		knownTypes = builder.build();

	}

	private static void add(ImmutableMap.Builder<Class<?>, AbstractJavaType> builder, AbstractJavaType jt) {

		builder.put(jt.getJavaClass(), jt);

		Optional<Class<?>> primitiveJavaClass = jt.getPrimitiveJavaClass();
		if (primitiveJavaClass.isPresent()) {
			builder.put(primitiveJavaClass.get(), jt);
		}

	}

	private MappingJavaTypes() {
	}

	public static AbstractJavaType resolveJavaType(Class<?> javaClass) {

		AbstractJavaType ajt = knownTypes.get(javaClass);
		if (ajt != null) {
			return ajt;
		}

		if (Enum.class.isAssignableFrom(javaClass)) {
			return ENUM_JAVA_TYPE;
		}

		if (TUPLE_VALUE_JAVA_TYPE.isApplicable(javaClass)) {
			return TUPLE_VALUE_JAVA_TYPE;
		}

		if (UDT_VALUE_JAVA_TYPE.isApplicable(javaClass)) {
			return UDT_VALUE_JAVA_TYPE;
		}

		throw new HelenusMappingException("unknown java type " + javaClass);
	}

	public final static class BooleanJavaType extends AbstractJavaType {

		@Override
		public Class<?> getJavaClass() {
			return Boolean.class;
		}

		@Override
		public Optional<Class<?>> getPrimitiveJavaClass() {
			return Optional.of(boolean.class);
		}

		@Override
		public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType, Metadata metadata) {
			return new DTDataType(columnType, DataType.cboolean());
		}

	}

	public final static class BigDecimalJavaType extends AbstractJavaType {

		@Override
		public Class<?> getJavaClass() {
			return BigDecimal.class;
		}

		@Override
		public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType, Metadata metadata) {
			return new DTDataType(columnType, DataType.decimal());
		}

	}

	public final static class BigIntegerJavaType extends AbstractJavaType {

		@Override
		public Class<?> getJavaClass() {
			return BigInteger.class;
		}

		@Override
		public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType, Metadata metadata) {
			return new DTDataType(columnType, DataType.varint());
		}

	}

	public final static class DoubleJavaType extends AbstractJavaType {

		@Override
		public Class<?> getJavaClass() {
			return Double.class;
		}

		@Override
		public Optional<Class<?>> getPrimitiveJavaClass() {
			return Optional.of(double.class);
		}

		@Override
		public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType, Metadata metadata) {
			return new DTDataType(columnType, DataType.cdouble());
		}

	}

	public final static class FloatJavaType extends AbstractJavaType {

		@Override
		public Class<?> getJavaClass() {
			return Float.class;
		}

		@Override
		public Optional<Class<?>> getPrimitiveJavaClass() {
			return Optional.of(float.class);
		}

		@Override
		public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType, Metadata metadata) {
			return new DTDataType(columnType, DataType.cfloat());
		}

	}

	public final static class IntegerJavaType extends AbstractJavaType {

		@Override
		public Class<?> getJavaClass() {
			return Integer.class;
		}

		@Override
		public Optional<Class<?>> getPrimitiveJavaClass() {
			return Optional.of(int.class);
		}

		@Override
		public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType, Metadata metadata) {
			return new DTDataType(columnType, DataType.cint());
		}

	}

	public final static class InetAddressJavaType extends AbstractJavaType {

		@Override
		public Class<?> getJavaClass() {
			return InetAddress.class;
		}

		@Override
		public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType, Metadata metadata) {
			return new DTDataType(columnType, DataType.inet());
		}

	}
}
