/*
 *
 *      Copyright (C) 2015 The Helenus Authors
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

import com.datastax.driver.core.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import java.lang.reflect.Method;
import java.util.*;
import net.helenus.config.HelenusSettings;
import net.helenus.core.Helenus;
import net.helenus.core.annotation.Cacheable;
import net.helenus.mapping.annotation.*;
import net.helenus.support.HelenusMappingException;
import org.apache.commons.lang3.ClassUtils;

public final class HelenusMappingEntity implements HelenusEntity {

  private final Class<?> iface;
  private final HelenusEntityType type;
  private final IdentityName name;
  private final boolean cacheable;
  private final ImmutableMap<String, Method> methods;
  private final ImmutableMap<String, HelenusProperty> props;
  private final ImmutableList<HelenusProperty> orderedProps;

  public HelenusMappingEntity(Class<?> iface, Metadata metadata) {
    this(iface, autoDetectType(iface), metadata);
  }

  public HelenusMappingEntity(Class<?> iface, HelenusEntityType type, Metadata metadata) {

    if (iface == null || !iface.isInterface()) {
      throw new IllegalArgumentException("invalid parameter " + iface);
    }

    this.iface = iface;
    this.type = Objects.requireNonNull(type, "type is empty");
    this.name = resolveName(iface, type);

    HelenusSettings settings = Helenus.settings();

    Map<String, Method> methods = new HashMap<String, Method>();
    for (Method m : iface.getDeclaredMethods()) {
        methods.put(m.getName(), m);
    }

    for (Class<?> c : ClassUtils.getAllInterfaces(iface)) {
      if (c.getDeclaredAnnotation(Table.class) != null || c.getDeclaredAnnotation(InheritedTable.class) != null) {
        for (Method m : c.getDeclaredMethods()) {
            Method o = methods.get(m.getName());
            if (o != null) {
                // Prefer overridden method implementation.
                if (o.getDeclaringClass().isAssignableFrom(m.getDeclaringClass())) {
                    methods.put(m.getName(), m);
                }
            } else {
                methods.put(m.getName(), m);
            }
        }
      }
    }

    List<HelenusProperty> propsLocal = new ArrayList<HelenusProperty>();
    ImmutableMap.Builder<String, HelenusProperty> propsBuilder = ImmutableMap.builder();
    ImmutableMap.Builder<String, Method> methodsBuilder = ImmutableMap.builder();

    for (Method method : methods.values()) {

      if (settings.getGetterMethodDetector().apply(method)) {

        methodsBuilder.put(method.getName(), method);

        if (metadata != null) {
          HelenusProperty prop = new HelenusMappingProperty(this, method, metadata);

          propsBuilder.put(prop.getPropertyName(), prop);
          propsLocal.add(prop);
        }
      }
    }

    this.methods = methodsBuilder.build();
    this.props = propsBuilder.build();

    Collections.sort(propsLocal, TypeAndOrdinalColumnComparator.INSTANCE);
    this.orderedProps = ImmutableList.copyOf(propsLocal);

    validateOrdinals();

    cacheable = (null != iface.getDeclaredAnnotation(Cacheable.class));
  }

  @Override
  public HelenusEntityType getType() {
    return type;
  }

  @Override
  public boolean isCacheable() {
    return cacheable;
  }

  @Override
  public Class<?> getMappingInterface() {
    return iface;
  }

  @Override
  public Collection<HelenusProperty> getOrderedProperties() {
    return orderedProps;
  }

  @Override
  public HelenusProperty getProperty(String name) {
    HelenusProperty property = props.get(name);
    if (property == null && methods.containsKey(name)) {
      property = new HelenusMappingProperty(this, methods.get(name), new DefaultMetadata());
      return property; //TODO(gburd): review adding these into the props map...
    }
    return props.get(name);
  }

  @Override
  public IdentityName getName() {
    return name;
  }

  private static IdentityName resolveName(Class<?> iface, HelenusEntityType type) {

    switch (type) {
      case TABLE:
        return MappingUtil.getTableName(iface, true);

      case TUPLE:
        return IdentityName.of(MappingUtil.getDefaultEntityName(iface), false);

      case UDT:
        return MappingUtil.getUserDefinedTypeName(iface, true);
    }

    throw new HelenusMappingException("invalid entity type " + type + " in " + type);
  }

  private static HelenusEntityType autoDetectType(Class<?> iface) {

    Objects.requireNonNull(iface, "empty iface");

    if (null != iface.getDeclaredAnnotation(Table.class)) {
      return HelenusEntityType.TABLE;
    } else if (null != iface.getDeclaredAnnotation(Tuple.class)) {
      return HelenusEntityType.TUPLE;
    } else if (null != iface.getDeclaredAnnotation(UDT.class)) {
      return HelenusEntityType.UDT;
    }

    throw new HelenusMappingException(
        "entity must be annotated by @Table or @Tuple or @UserDefinedType " + iface);
  }

  private void validateOrdinals() {

    switch (getType()) {
      case TABLE:
        validateOrdinalsForTable();
        break;

      case TUPLE:
        validateOrdinalsInTuple();
        break;

      default:
        break;
    }
  }

  private void validateOrdinalsForTable() {

    BitSet partitionKeys = new BitSet();
    BitSet clusteringColumns = new BitSet();

    for (HelenusProperty prop : getOrderedProperties()) {

      ColumnType type = prop.getColumnType();

      int ordinal = prop.getOrdinal();

      switch (type) {
        case PARTITION_KEY:
          if (partitionKeys.get(ordinal)) {
            throw new HelenusMappingException(
                "detected two or more partition key columns with the same ordinal "
                    + ordinal
                    + " in "
                    + prop.getEntity());
          }
          partitionKeys.set(ordinal);
          break;

        case CLUSTERING_COLUMN:
          if (clusteringColumns.get(ordinal)) {
            throw new HelenusMappingException(
                "detected two or clustering columns with the same ordinal "
                    + ordinal
                    + " in "
                    + prop.getEntity());
          }
          clusteringColumns.set(ordinal);
          break;

        default:
          break;
      }
    }
  }

  private void validateOrdinalsInTuple() {
    boolean[] ordinals = new boolean[props.size()];

    getOrderedProperties()
        .forEach(
            p -> {
              int ordinal = p.getOrdinal();

              if (ordinal < 0 || ordinal >= ordinals.length) {
                throw new HelenusMappingException(
                    "invalid ordinal "
                        + ordinal
                        + " found for property "
                        + p.getPropertyName()
                        + " in "
                        + p.getEntity());
              }

              if (ordinals[ordinal]) {
                throw new HelenusMappingException(
                    "detected two or more properties with the same ordinal "
                        + ordinal
                        + " in "
                        + p.getEntity());
              }

              ordinals[ordinal] = true;
            });

    for (int i = 0; i != ordinals.length; ++i) {
      if (!ordinals[i]) {
        throw new HelenusMappingException("detected absent ordinal " + i + " in " + this);
      }
    }
  }

  @Override
  public String toString() {

    StringBuilder str = new StringBuilder();
    str.append(iface.getSimpleName())
        .append("(")
        .append(name.getName())
        .append(") ")
        .append(type.name().toLowerCase())
        .append(":\n");

    for (HelenusProperty prop : getOrderedProperties()) {
      str.append(prop.toString());
      str.append("\n");
    }
    return str.toString();
  }
}
