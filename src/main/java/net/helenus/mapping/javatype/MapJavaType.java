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
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import net.helenus.core.SessionRepository;
import net.helenus.mapping.ColumnType;
import net.helenus.mapping.IdentityName;
import net.helenus.mapping.annotation.Types;
import net.helenus.mapping.convert.tuple.*;
import net.helenus.mapping.convert.udt.*;
import net.helenus.mapping.type.*;
import net.helenus.support.Either;
import net.helenus.support.HelenusMappingException;

public final class MapJavaType extends AbstractJavaType {

  @Override
  public Class<?> getJavaClass() {
    return Map.class;
  }

  @Override
  public AbstractDataType resolveDataType(
      Method getter, Type genericJavaType, ColumnType columnType, Metadata metadata) {

    Types.Map cmap = getter.getDeclaredAnnotation(Types.Map.class);
    if (cmap != null) {
      return new DTDataType(
          columnType,
          DataType.map(
              resolveSimpleType(getter, cmap.key()), resolveSimpleType(getter, cmap.value())));
    }

    Types.UDTKeyMap udtKeyMap = getter.getDeclaredAnnotation(Types.UDTKeyMap.class);
    if (udtKeyMap != null) {
      return new UDTKeyMapDataType(
          columnType,
          resolveUDT(udtKeyMap.key()),
          UDTValue.class,
          resolveSimpleType(getter, udtKeyMap.value()));
    }

    Types.UDTValueMap udtValueMap = getter.getDeclaredAnnotation(Types.UDTValueMap.class);
    if (udtValueMap != null) {
      return new UDTValueMapDataType(
          columnType,
          resolveSimpleType(getter, udtValueMap.key()),
          resolveUDT(udtValueMap.value()),
          UDTValue.class);
    }

    Types.UDTMap udtMap = getter.getDeclaredAnnotation(Types.UDTMap.class);
    if (udtMap != null) {
      return new UDTMapDataType(
          columnType,
          resolveUDT(udtMap.key()),
          UDTValue.class,
          resolveUDT(udtMap.value()),
          UDTValue.class);
    }

    Type[] args = getTypeParameters(genericJavaType);
    ensureTypeArguments(getter, args.length, 2);

    Either<DataType, IdentityName> key = autodetectParameterType(getter, args[0], metadata);
    Either<DataType, IdentityName> value = autodetectParameterType(getter, args[1], metadata);

    if (key.isLeft()) {

      if (value.isLeft()) {
        return DTDataType.map(columnType, key.getLeft(), args[0], value.getLeft(), args[1]);
      } else {
        return new UDTValueMapDataType(
            columnType, key.getLeft(), value.getRight(), (Class<?>) args[1]);
      }
    } else {

      if (value.isLeft()) {
        return new UDTKeyMapDataType(
            columnType, key.getRight(), (Class<?>) args[0], value.getLeft());
      } else {
        return new UDTMapDataType(
            columnType, key.getRight(), (Class<?>) args[0], value.getRight(), (Class<?>) args[1]);
      }
    }
  }

  @Override
  public Optional<Function<Object, Object>> resolveReadConverter(
      AbstractDataType abstractDataType, SessionRepository repository) {

    if (abstractDataType instanceof DTDataType) {
      return resolveDTReadConverter((DTDataType) abstractDataType, repository);
    } else if (abstractDataType instanceof UDTKeyMapDataType) {

      UDTKeyMapDataType dt = (UDTKeyMapDataType) abstractDataType;

      Class<Object> keyClass = (Class<Object>) dt.getUdtKeyClass();

      if (UDTValue.class.isAssignableFrom(keyClass)) {
        return Optional.empty();
      }

      return Optional.of(new UDTKeyMapToMapConverter(keyClass, repository));

    } else if (abstractDataType instanceof UDTValueMapDataType) {

      UDTValueMapDataType dt = (UDTValueMapDataType) abstractDataType;

      Class<Object> valueClass = (Class<Object>) dt.getUdtValueClass();

      if (UDTValue.class.isAssignableFrom(valueClass)) {
        return Optional.empty();
      }

      return Optional.of(new UDTValueMapToMapConverter(valueClass, repository));

    } else if (abstractDataType instanceof UDTMapDataType) {

      UDTMapDataType dt = (UDTMapDataType) abstractDataType;

      Class<Object> keyClass = (Class<Object>) dt.getUdtKeyClass();
      Class<Object> valueClass = (Class<Object>) dt.getUdtValueClass();

      if (UDTValue.class.isAssignableFrom(keyClass)) {

        if (UDTValue.class.isAssignableFrom(valueClass)) {
          return Optional.empty();
        } else {
          return Optional.of(new UDTValueMapToMapConverter(valueClass, repository));
        }
      } else if (UDTValue.class.isAssignableFrom(valueClass)) {
        return Optional.of(new UDTKeyMapToMapConverter(keyClass, repository));
      }

      return Optional.of(new UDTMapToMapConverter(keyClass, valueClass, repository));
    }

    return Optional.empty();
  }

  private Optional<Function<Object, Object>> resolveDTReadConverter(
      DTDataType dt, SessionRepository repository) {

    DataType keyDataType = dt.getDataType().getTypeArguments().get(0);
    DataType valueDataType = dt.getDataType().getTypeArguments().get(1);

    if (keyDataType instanceof TupleType) {

      if (valueDataType instanceof TupleType) {

        Class<?> keyClass = dt.getTypeArguments()[0];
        Class<?> valueClass = dt.getTypeArguments()[1];

        if (TupleValue.class.isAssignableFrom(keyClass)) {

          if (TupleValue.class.isAssignableFrom(valueClass)) {
            return Optional.empty();
          } else {
            return Optional.of(new TupleValueMapToMapConverter(valueClass, repository));
          }

        } else if (TupleValue.class.isAssignableFrom(valueClass)) {
          return Optional.of(new TupleKeyMapToMapConverter(keyClass, repository));
        }

        return Optional.of(new TupleMapToMapConverter(keyClass, valueClass, repository));

      } else {
        Class<?> keyClass = dt.getTypeArguments()[0];

        if (TupleValue.class.isAssignableFrom(keyClass)) {
          return Optional.empty();
        }

        return Optional.of(new TupleKeyMapToMapConverter(keyClass, repository));
      }
    } else if (valueDataType instanceof TupleType) {

      Class<?> valueClass = dt.getTypeArguments()[0];

      if (TupleValue.class.isAssignableFrom(valueClass)) {
        return Optional.empty();
      }

      return Optional.of(new TupleValueMapToMapConverter(valueClass, repository));
    }

    return Optional.empty();
  }

  @Override
  public Optional<Function<Object, Object>> resolveWriteConverter(
      AbstractDataType abstractDataType, SessionRepository repository) {

    if (abstractDataType instanceof DTDataType) {
      return resolveDTWriteConverter((DTDataType) abstractDataType, repository);
    } else if (abstractDataType instanceof UDTKeyMapDataType) {

      UDTKeyMapDataType dt = (UDTKeyMapDataType) abstractDataType;

      Class<Object> keyClass = (Class<Object>) dt.getUdtKeyClass();

      if (UDTValue.class.isAssignableFrom(keyClass)) {
        return Optional.empty();
      }

      return Optional.of(
          new MapToUDTKeyMapConverter(
              keyClass, getUserType(dt.getUdtKeyName(), keyClass, repository), repository));

    } else if (abstractDataType instanceof UDTValueMapDataType) {

      UDTValueMapDataType dt = (UDTValueMapDataType) abstractDataType;

      Class<Object> valueClass = (Class<Object>) dt.getUdtValueClass();

      if (UDTValue.class.isAssignableFrom(valueClass)) {
        return Optional.empty();
      }

      return Optional.of(
          new MapToUDTValueMapConverter(
              valueClass, getUserType(dt.getUdtValueName(), valueClass, repository), repository));

    } else if (abstractDataType instanceof UDTMapDataType) {

      UDTMapDataType dt = (UDTMapDataType) abstractDataType;

      Class<Object> keyClass = (Class<Object>) dt.getUdtKeyClass();
      Class<Object> valueClass = (Class<Object>) dt.getUdtValueClass();

      if (UDTValue.class.isAssignableFrom(keyClass)) {

        if (UDTValue.class.isAssignableFrom(valueClass)) {
          return Optional.empty();
        } else {

          return Optional.of(
              new MapToUDTValueMapConverter(
                  valueClass,
                  getUserType(dt.getUdtValueName(), valueClass, repository),
                  repository));
        }
      } else if (UDTValue.class.isAssignableFrom(valueClass)) {

        return Optional.of(
            new MapToUDTKeyMapConverter(
                keyClass, getUserType(dt.getUdtKeyName(), keyClass, repository), repository));
      }

      return Optional.of(
          new MapToUDTMapConverter(
              keyClass,
              getUserType(dt.getUdtKeyName(), keyClass, repository),
              valueClass,
              getUserType(dt.getUdtValueName(), valueClass, repository),
              repository));
    }

    return Optional.empty();
  }

  private Optional<Function<Object, Object>> resolveDTWriteConverter(
      DTDataType dt, SessionRepository repository) {

    DataType keyDataType = dt.getDataType().getTypeArguments().get(0);
    DataType valueDataType = dt.getDataType().getTypeArguments().get(1);

    if (keyDataType instanceof TupleType) {

      if (valueDataType instanceof TupleType) {

        Class<?> keyClass = dt.getTypeArguments()[0];
        Class<?> valueClass = dt.getTypeArguments()[1];

        if (TupleValue.class.isAssignableFrom(keyClass)) {

          if (TupleValue.class.isAssignableFrom(valueClass)) {
            return Optional.empty();
          } else {
            return Optional.of(
                new MapToTupleValueMapConverter(valueClass, (TupleType) valueDataType, repository));
          }

        } else if (TupleValue.class.isAssignableFrom(valueClass)) {

          return Optional.of(
              new MapToTupleKeyMapConverter(keyClass, (TupleType) keyDataType, repository));
        }

        return Optional.of(
            new MapToTupleMapConverter(
                keyClass,
                (TupleType) keyDataType,
                valueClass,
                (TupleType) valueDataType,
                repository));

      } else {

        Class<?> keyClass = dt.getTypeArguments()[0];

        if (TupleValue.class.isAssignableFrom(keyClass)) {
          return Optional.empty();
        }

        return Optional.of(
            new MapToTupleKeyMapConverter(keyClass, (TupleType) keyDataType, repository));
      }
    } else if (valueDataType instanceof TupleType) {

      Class<?> valueClass = dt.getTypeArguments()[0];

      if (TupleValue.class.isAssignableFrom(valueClass)) {
        return Optional.empty();
      }

      return Optional.of(
          new MapToTupleValueMapConverter(valueClass, (TupleType) valueDataType, repository));
    }

    return Optional.empty();
  }

  private UserType getUserType(
      IdentityName name, Class<?> javaClass, SessionRepository repository) {
    UserType userType = repository.findUserType(name.getName());
    if (userType == null) {
      throw new HelenusMappingException(
          "UserType not found for " + name + " with type " + javaClass);
    }
    return userType;
  }
}
