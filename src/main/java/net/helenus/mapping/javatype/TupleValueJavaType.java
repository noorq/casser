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

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import net.helenus.core.Helenus;
import net.helenus.core.SessionRepository;
import net.helenus.mapping.ColumnType;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.IdentityName;
import net.helenus.mapping.MappingUtil;
import net.helenus.mapping.annotation.Types;
import net.helenus.mapping.convert.TypedConverter;
import net.helenus.mapping.convert.tuple.EntityToTupleValueConverter;
import net.helenus.mapping.convert.tuple.TupleValueToEntityConverter;
import net.helenus.mapping.type.AbstractDataType;
import net.helenus.mapping.type.DTDataType;
import net.helenus.support.HelenusMappingException;

public final class TupleValueJavaType extends AbstractJavaType {

  public static TupleType toTupleType(Class<?> javaType, Metadata metadata) {
    HelenusEntity tupleEntity = Helenus.entity(javaType, metadata);

    List<DataType> tupleTypes =
        tupleEntity
            .getOrderedProperties()
            .stream()
            .map(p -> p.getDataType())
            .filter(d -> d instanceof DTDataType)
            .map(d -> (DTDataType) d)
            .map(d -> d.getDataType())
            .collect(Collectors.toList());

    if (tupleTypes.size() < tupleEntity.getOrderedProperties().size()) {

      List<IdentityName> wrongColumns =
          tupleEntity
              .getOrderedProperties()
              .stream()
              .filter(p -> !(p.getDataType() instanceof DTDataType))
              .map(p -> p.getColumnName())
              .collect(Collectors.toList());

      throw new HelenusMappingException(
          "non simple types in tuple "
              + tupleEntity.getMappingInterface()
              + " in columns: "
              + wrongColumns);
    }

    return metadata.newTupleType(tupleTypes.toArray(new DataType[tupleTypes.size()]));
  }

  @Override
  public Class<?> getJavaClass() {
    return TupleValue.class;
  }

  @Override
  public boolean isApplicable(Class<?> javaClass) {
    return MappingUtil.isTuple(javaClass);
  }

  @Override
  public AbstractDataType resolveDataType(
      Method getter, Type genericJavaType, ColumnType columnType, Metadata metadata) {

    Class<?> javaType = (Class<?>) genericJavaType;

    if (TupleValue.class.isAssignableFrom(javaType)) {

      Types.Tuple tuple = getter.getDeclaredAnnotation(Types.Tuple.class);
      if (tuple == null) {
        throw new HelenusMappingException(
            "tuple must be annotated by @Tuple annotation in " + getter);
      }

      DataType.Name[] tupleArguments = tuple.value();
      int len = tupleArguments.length;
      DataType[] arguments = new DataType[len];

      for (int i = 0; i != len; ++i) {
        arguments[i] = resolveSimpleType(getter, tupleArguments[i]);
      }

      TupleType tupleType = metadata.newTupleType(arguments);
      return new DTDataType(columnType, tupleType, javaType);

    } else {
      return new DTDataType(columnType, toTupleType(javaType, metadata), javaType);
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

    return Optional.of(
        TypedConverter.create(
            TupleValue.class, javaClass, new TupleValueToEntityConverter(javaClass, repository)));
  }

  @Override
  public Optional<Function<Object, Object>> resolveWriteConverter(
      AbstractDataType dataType, SessionRepository repository) {

    DTDataType dt = (DTDataType) dataType;

    Class<Object> javaClass = (Class<Object>) dt.getJavaClass();

    if (TupleValue.class.isAssignableFrom(javaClass)) {
      return Optional.empty();
    }

    return Optional.of(
        TypedConverter.create(
            javaClass,
            TupleValue.class,
            new EntityToTupleValueConverter(javaClass, (TupleType) dt.getDataType(), repository)));
  }
}
