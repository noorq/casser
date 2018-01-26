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
package net.helenus.core;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.Multimap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import net.helenus.mapping.HelenusEntity;
import net.helenus.mapping.HelenusEntityType;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.type.AbstractDataType;
import net.helenus.mapping.type.DTDataType;
import net.helenus.support.HelenusMappingException;

public final class SessionRepositoryBuilder {

  private static final Optional<HelenusEntityType> OPTIONAL_UDT =
      Optional.of(HelenusEntityType.UDT);

  private final Map<Class<?>, HelenusEntity> entityMap = new HashMap<Class<?>, HelenusEntity>();

  private final Map<String, UserType> userTypeMap = new HashMap<String, UserType>();

  private final Multimap<HelenusEntity, HelenusEntity> userTypeUsesMap = HashMultimap.create();

  private final Session session;

  SessionRepositoryBuilder(Session session) {
    this.session = session;
  }

  public SessionRepository build() {
    return new SessionRepository(this);
  }

  public Collection<HelenusEntity> getUserTypeUses(HelenusEntity udtName) {
    return userTypeUsesMap.get(udtName);
  }

  public Collection<HelenusEntity> entities() {
    return entityMap.values();
  }

  protected Map<Class<?>, HelenusEntity> getEntityMap() {
    return entityMap;
  }

  protected Map<String, UserType> getUserTypeMap() {
    return userTypeMap;
  }

  public void addUserType(String name, UserType userType) {
    userTypeMap.putIfAbsent(name.toLowerCase(), userType);
  }

  public HelenusEntity add(Object dsl) {
    return add(dsl, Optional.empty());
  }

  public void addEntity(HelenusEntity entity) {

    HelenusEntity concurrentEntity = entityMap.putIfAbsent(entity.getMappingInterface(), entity);

    if (concurrentEntity == null) {
      addUserDefinedTypes(entity.getOrderedProperties());
    }
  }

  public HelenusEntity add(Object dsl, Optional<HelenusEntityType> type) {

    HelenusEntity helenusEntity = Helenus.resolve(dsl, session.getCluster().getMetadata());

    Class<?> iface = helenusEntity.getMappingInterface();

    HelenusEntity entity = entityMap.get(iface);

    if (entity == null) {

      entity = helenusEntity;

      if (type.isPresent() && entity.getType() != type.get()) {
        throw new HelenusMappingException(
            "unexpected entity type " + entity.getType() + " for " + entity);
      }

      HelenusEntity concurrentEntity = entityMap.putIfAbsent(iface, entity);

      if (concurrentEntity == null) {
        addUserDefinedTypes(entity.getOrderedProperties());
      } else {
        entity = concurrentEntity;
      }
    }

    return entity;
  }

  private void addUserDefinedTypes(Collection<HelenusProperty> props) {

    for (HelenusProperty prop : props) {

      AbstractDataType type = prop.getDataType();

      if (type instanceof DTDataType) {
        continue;
      }

      if (!UDTValue.class.isAssignableFrom(prop.getJavaType())) {

        for (Class<?> udtClass : type.getTypeArguments()) {

          if (UDTValue.class.isAssignableFrom(udtClass)) {
            continue;
          }

          HelenusEntity addedUserType = add(udtClass, OPTIONAL_UDT);

          if (HelenusEntityType.UDT == prop.getEntity().getType()) {
            userTypeUsesMap.put(prop.getEntity(), addedUserType);
          }
        }
      }
    }
  }
}
