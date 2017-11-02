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

import com.datastax.driver.core.*;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import net.helenus.core.SessionRepository;
import net.helenus.mapping.ColumnType;
import net.helenus.mapping.IdentityName;
import net.helenus.mapping.annotation.Types;
import net.helenus.mapping.convert.tuple.TupleListToListConverter;
import net.helenus.mapping.convert.udt.ListToUDTListConverter;
import net.helenus.mapping.convert.udt.UDTListToListConverter;
import net.helenus.mapping.type.AbstractDataType;
import net.helenus.mapping.type.DTDataType;
import net.helenus.mapping.type.ListToTupleListConverter;
import net.helenus.mapping.type.UDTListDataType;
import net.helenus.support.Either;
import net.helenus.support.HelenusMappingException;

public final class ListJavaType extends AbstractCollectionJavaType {

  @Override
  public Class<?> getJavaClass() {
    return List.class;
  }

  @Override
  public AbstractDataType resolveDataType(
      Method getter, Type genericJavaType, ColumnType columnType, Metadata metadata) {

    Types.List clist = getter.getDeclaredAnnotation(Types.List.class);
    if (clist != null) {
      return new DTDataType(columnType, DataType.list(resolveSimpleType(getter, clist.value())));
    }

    Types.UDTList udtList = getter.getDeclaredAnnotation(Types.UDTList.class);
    if (udtList != null) {
      return new UDTListDataType(columnType, resolveUDT(udtList.value()), UDTValue.class);
    }

    Type[] args = getTypeParameters(genericJavaType);
    ensureTypeArguments(getter, args.length, 1);

    Either<DataType, IdentityName> parameterType =
        autodetectParameterType(getter, args[0], metadata);

    if (parameterType.isLeft()) {
      return DTDataType.list(columnType, parameterType.getLeft(), args[0]);
    } else {
      return new UDTListDataType(columnType, parameterType.getRight(), (Class<?>) args[0]);
    }
  }

  @Override
  public Optional<Function<Object, Object>> resolveReadConverter(
      AbstractDataType abstractDataType, SessionRepository repository) {

    if (abstractDataType instanceof DTDataType) {

      DTDataType dt = (DTDataType) abstractDataType;
      DataType elementType = dt.getDataType().getTypeArguments().get(0);
      if (elementType instanceof TupleType) {

        Class<?> tupleClass = dt.getTypeArguments()[0];

        if (TupleValue.class.isAssignableFrom(tupleClass)) {
          return Optional.empty();
        }

        return Optional.of(new TupleListToListConverter(tupleClass, repository));
      }
    } else if (abstractDataType instanceof UDTListDataType) {

      UDTListDataType dt = (UDTListDataType) abstractDataType;

      Class<Object> javaClass = (Class<Object>) dt.getTypeArguments()[0];

      if (UDTValue.class.isAssignableFrom(javaClass)) {
        return Optional.empty();
      }

      return Optional.of(new UDTListToListConverter(javaClass, repository));
    }

    return Optional.empty();
  }

  @Override
  public Optional<Function<Object, Object>> resolveWriteConverter(
      AbstractDataType abstractDataType, SessionRepository repository) {

    if (abstractDataType instanceof DTDataType) {

      DTDataType dt = (DTDataType) abstractDataType;
      DataType elementType = dt.getDataType().getTypeArguments().get(0);

      if (elementType instanceof TupleType) {

        Class<?> tupleClass = dt.getTypeArguments()[0];

        if (TupleValue.class.isAssignableFrom(tupleClass)) {
          return Optional.empty();
        }

        return Optional.of(
            new ListToTupleListConverter(tupleClass, (TupleType) elementType, repository));
      }

    } else if (abstractDataType instanceof UDTListDataType) {

      UDTListDataType dt = (UDTListDataType) abstractDataType;

      Class<Object> javaClass = (Class<Object>) dt.getTypeArguments()[0];

      if (UDTValue.class.isAssignableFrom(javaClass)) {
        return Optional.empty();
      }

      UserType userType = repository.findUserType(dt.getUdtName().getName());
      if (userType == null) {
        throw new HelenusMappingException(
            "UserType not found for " + dt.getUdtName() + " with type " + javaClass);
      }

      return Optional.of(new ListToUDTListConverter(javaClass, userType, repository));
    }

    return Optional.empty();
  }
}
