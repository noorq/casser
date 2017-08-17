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
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import javax.validation.Constraint;
import javax.validation.ConstraintValidator;
import net.helenus.core.Getter;
import net.helenus.core.Helenus;
import net.helenus.core.reflect.*;
import net.helenus.mapping.annotation.Index;
import net.helenus.mapping.annotation.Table;
import net.helenus.mapping.annotation.Tuple;
import net.helenus.mapping.annotation.UDT;
import net.helenus.support.DslPropertyException;
import net.helenus.support.HelenusMappingException;

public final class MappingUtil {

  @SuppressWarnings("unchecked")
  public static final ConstraintValidator<? extends Annotation, ?>[] EMPTY_VALIDATORS =
      new ConstraintValidator[0];

  private MappingUtil() {}

  public static ConstraintValidator<? extends Annotation, ?>[] getValidators(Method getterMethod) {

    List<ConstraintValidator<? extends Annotation, ?>> list = null;

    for (Annotation constraintAnnotation : getterMethod.getDeclaredAnnotations()) {

      list = addValidators(constraintAnnotation, list);

      Class<? extends Annotation> annotationType = constraintAnnotation.annotationType();

      for (Annotation possibleConstraint : annotationType.getDeclaredAnnotations()) {

        list = addValidators(possibleConstraint, list);
      }
    }

    if (list == null) {
      return EMPTY_VALIDATORS;
    } else {
      return list.toArray(EMPTY_VALIDATORS);
    }
  }

  private static List<ConstraintValidator<? extends Annotation, ?>> addValidators(
      Annotation constraintAnnotation, List<ConstraintValidator<? extends Annotation, ?>> list) {

    Class<? extends Annotation> annotationType = constraintAnnotation.annotationType();

    for (Annotation possibleConstraint : annotationType.getDeclaredAnnotations()) {

      if (possibleConstraint instanceof Constraint) {

        Constraint constraint = (Constraint) possibleConstraint;

        for (Class<? extends ConstraintValidator<?, ?>> clazz : constraint.validatedBy()) {

          ConstraintValidator<? extends Annotation, ?> validator =
              ReflectionInstantiator.instantiateClass(clazz);

          ((ConstraintValidator) validator).initialize(constraintAnnotation);

          if (list == null) {
            list = new ArrayList<ConstraintValidator<? extends Annotation, ?>>();
          }

          list.add(validator);
        }
      }
    }

    return list;
  }

  public static Optional<IdentityName> getIndexName(Method getterMethod) {

    String indexName = null;
    boolean forceQuote = false;

    Index index = getterMethod.getDeclaredAnnotation(Index.class);

    if (index != null) {
      indexName = index.value();
      forceQuote = index.forceQuote();

      if (indexName == null || indexName.isEmpty()) {
        indexName = getDefaultColumnName(getterMethod);
      }
    }

    return indexName != null
        ? Optional.of(new IdentityName(indexName, forceQuote))
        : Optional.empty();
  }

  public static boolean caseSensitiveIndex(Method getterMethod) {
    Index index = getterMethod.getDeclaredAnnotation(Index.class);

    if (index != null) {
      return index.caseSensitive();
    }

    return false;
  }

  public static String getPropertyName(Method getter) {
    return getter.getName();
  }

  public static String getDefaultColumnName(Method getter) {
    return Helenus.settings().getPropertyToColumnConverter().apply(getPropertyName(getter));
  }

  public static IdentityName getUserDefinedTypeName(Class<?> iface, boolean required) {

    String userTypeName = null;
    boolean forceQuote = false;

    UDT userDefinedType = iface.getDeclaredAnnotation(UDT.class);

    if (userDefinedType != null) {

      userTypeName = userDefinedType.value();
      forceQuote = userDefinedType.forceQuote();

      if (userTypeName == null || userTypeName.isEmpty()) {
        userTypeName = getDefaultEntityName(iface);
      }

      return new IdentityName(userTypeName, forceQuote);
    }

    if (required) {
      throw new HelenusMappingException("entity must have annotation @UserDefinedType " + iface);
    }

    return null;
  }

  public static boolean isTuple(Class<?> iface) {

    Tuple tuple = iface.getDeclaredAnnotation(Tuple.class);

    return tuple != null;
  }

  public static boolean isUDT(Class<?> iface) {

    UDT udt = iface.getDeclaredAnnotation(UDT.class);

    return udt != null;
  }

  public static IdentityName getTableName(Class<?> iface, boolean required) {

    String tableName = null;
    boolean forceQuote = false;

    Table table = iface.getDeclaredAnnotation(Table.class);

    if (table != null) {
      tableName = table.value();
      forceQuote = table.forceQuote();

    } else if (required) {
      throw new HelenusMappingException("entity must have annotation @Table " + iface);
    }

    if (tableName == null || tableName.isEmpty()) {
      tableName = getDefaultEntityName(iface);
    }

    return new IdentityName(tableName, forceQuote);
  }

  public static String getDefaultEntityName(Class<?> iface) {
    return Helenus.settings().getPropertyToColumnConverter().apply(iface.getSimpleName());
  }

  public static Class<?> getMappingInterface(Object pojo) {

    Class<?> iface = null;

    if (pojo instanceof Class) {
      iface = (Class<?>) pojo;

      if (!iface.isInterface()) {
        throw new HelenusMappingException("expected interface " + iface);
      }

    } else {
      Class<?>[] ifaces = pojo.getClass().getInterfaces();

      int len = ifaces.length;
      for (int i = 0; i != len; ++i) {

        iface = ifaces[0];

        if (MapExportable.class.isAssignableFrom(iface)) {
          continue;
        }

        if (iface.getDeclaredAnnotation(Table.class) != null
            || iface.getDeclaredAnnotation(UDT.class) != null
            || iface.getDeclaredAnnotation(Tuple.class) != null) {

          break;
        }
      }
    }

    if (iface == null) {
      throw new HelenusMappingException("dsl interface not found for " + pojo);
    }

    return iface;
  }

  public static HelenusPropertyNode resolveMappingProperty(Getter<?> getter) {

    try {
      Object childDsl = getter.get();

      if (childDsl instanceof DslExportable) {
        DslExportable e = (DslExportable) childDsl;
        return e.getParentDslHelenusPropertyNode();
      } else if (childDsl instanceof MapDsl) {
        MapDsl mapDsl = (MapDsl) childDsl;
        return mapDsl.getParent();
      } else if (childDsl instanceof ListDsl) {
        ListDsl listDsl = (ListDsl) childDsl;
        return listDsl.getParent();
      } else if (childDsl instanceof SetDsl) {
        SetDsl setDsl = (SetDsl) childDsl;
        return setDsl.getParent();
      }

      throw new HelenusMappingException("getter must reference to the dsl object " + getter);

    } catch (DslPropertyException e) {
      return e.getPropertyNode();
    }
  }
}
