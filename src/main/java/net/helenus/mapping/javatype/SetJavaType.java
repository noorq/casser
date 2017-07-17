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
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.datastax.driver.core.*;

import net.helenus.core.SessionRepository;
import net.helenus.mapping.ColumnType;
import net.helenus.mapping.IdentityName;
import net.helenus.mapping.annotation.Types;
import net.helenus.mapping.convert.tuple.SetToTupleSetConverter;
import net.helenus.mapping.convert.tuple.TupleSetToSetConverter;
import net.helenus.mapping.convert.udt.SetToUDTSetConverter;
import net.helenus.mapping.convert.udt.UDTSetToSetConverter;
import net.helenus.mapping.type.AbstractDataType;
import net.helenus.mapping.type.DTDataType;
import net.helenus.mapping.type.UDTSetDataType;
import net.helenus.support.Either;
import net.helenus.support.HelenusMappingException;

public final class SetJavaType extends AbstractJavaType {

	@Override
	public Class<?> getJavaClass() {
		return Set.class;
	}

	@Override
	public AbstractDataType resolveDataType(Method getter, Type genericJavaType, ColumnType columnType, Metadata metadata) {

		Types.Set cset = getter.getDeclaredAnnotation(Types.Set.class);
		if (cset != null) {
			return new DTDataType(columnType, DataType.set(resolveSimpleType(getter, cset.value())));
		}

		Types.UDTSet udtSet = getter.getDeclaredAnnotation(Types.UDTSet.class);
		if (udtSet != null) {
			return new UDTSetDataType(columnType, resolveUDT(udtSet.value()), UDTValue.class);
		}

		Type[] args = getTypeParameters(genericJavaType);
		ensureTypeArguments(getter, args.length, 1);

		Either<DataType, IdentityName> parameterType = autodetectParameterType(getter, args[0], metadata);

		if (parameterType.isLeft()) {
			return DTDataType.set(columnType, parameterType.getLeft(), args[0]);
		} else {
			return new UDTSetDataType(columnType, parameterType.getRight(), (Class<?>) args[0]);
		}

	}

	@Override
	public Optional<Function<Object, Object>> resolveReadConverter(AbstractDataType abstractDataType,
			SessionRepository repository) {

		if (abstractDataType instanceof DTDataType) {

			DTDataType dt = (DTDataType) abstractDataType;
			DataType elementType = dt.getDataType().getTypeArguments().get(0);
			if (elementType instanceof TupleType) {

				Class<?> tupleClass = dt.getTypeArguments()[0];

				if (TupleValue.class.isAssignableFrom(tupleClass)) {
					return Optional.empty();
				}

				return Optional.of(new TupleSetToSetConverter(tupleClass, repository));
			}
		}

		else if (abstractDataType instanceof UDTSetDataType) {

			UDTSetDataType dt = (UDTSetDataType) abstractDataType;

			Class<Object> udtClass = (Class<Object>) dt.getTypeArguments()[0];

			if (UDTValue.class.isAssignableFrom(udtClass)) {
				return Optional.empty();
			}

			return Optional.of(new UDTSetToSetConverter(udtClass, repository));

		}

		return Optional.empty();
	}

	@Override
	public Optional<Function<Object, Object>> resolveWriteConverter(AbstractDataType abstractDataType,
			SessionRepository repository) {

		if (abstractDataType instanceof DTDataType) {

			DTDataType dt = (DTDataType) abstractDataType;
			DataType elementType = dt.getDataType().getTypeArguments().get(0);

			if (elementType instanceof TupleType) {

				Class<?> tupleClass = dt.getTypeArguments()[0];

				if (TupleValue.class.isAssignableFrom(tupleClass)) {
					return Optional.empty();
				}

				return Optional.of(new SetToTupleSetConverter(tupleClass, (TupleType) elementType, repository));
			}

		}

		else if (abstractDataType instanceof UDTSetDataType) {

			UDTSetDataType dt = (UDTSetDataType) abstractDataType;

			Class<Object> udtClass = (Class<Object>) dt.getTypeArguments()[0];

			if (UDTValue.class.isAssignableFrom(udtClass)) {
				return Optional.empty();
			}

			UserType userType = repository.findUserType(dt.getUdtName().getName());
			if (userType == null) {
				throw new HelenusMappingException(
						"UserType not found for " + dt.getUdtName() + " with type " + udtClass);
			}

			return Optional.of(new SetToUDTSetConverter(udtClass, userType, repository));

		}

		return Optional.empty();
	}

}
