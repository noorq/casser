package net.helenus.core;

import com.google.common.primitives.Primitives;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import net.helenus.core.reflect.DefaultPrimitiveTypes;
import net.helenus.core.reflect.Drafted;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.MappingUtil;
import org.apache.commons.lang3.SerializationUtils;

public abstract class AbstractEntityDraft<E> implements Drafted<E> {

  private final MapExportable entity;
  private final Map<String, Object> valuesMap;
  private final Set<String> readSet;
  private final Map<String, Object> mutationsMap = new HashMap<String, Object>();

  public AbstractEntityDraft(MapExportable entity) {
    this.entity = entity;
    // Entities can mutate their map.
    if (entity != null) {
      this.valuesMap = entity.toMap(true);
      this.readSet = entity.toReadSet();
    } else {
      this.valuesMap = new HashMap<String, Object>();
      this.readSet = new HashSet<String>();
    }
  }

  public abstract Class<E> getEntityClass();

  public E build() {
    return Helenus.map(getEntityClass(), toMap());
  }

  @SuppressWarnings("unchecked")
  public <T> T get(Getter<T> getter, Class<?> returnType) {
    return (T) get(this.<T>methodNameFor(getter), returnType);
  }

  @SuppressWarnings("unchecked")
  public <T> T get(String key, Class<?> returnType) {
    readSet.add(key);
    T value = (T) mutationsMap.get(key);

    if (value == null) {
      value = (T) valuesMap.get(key);
      if (value == null) {

        if (Primitives.allPrimitiveTypes().contains(returnType)) {

          DefaultPrimitiveTypes type = DefaultPrimitiveTypes.lookup(returnType);
          if (type == null) {
            throw new RuntimeException("unknown primitive type " + returnType);
          }

          return (T) type.getDefaultValue();
        }
      } else {
        // Collections fetched from the valuesMap
        if (value instanceof Collection) {
          value = (T) SerializationUtils.<Serializable>clone((Serializable) value);
        }
      }
    }

    return value;
  }

  public <T> Object set(Getter<T> getter, Object value) {
    HelenusProperty prop = MappingUtil.resolveMappingProperty(getter).getProperty();
    String key = prop.getPropertyName();

    HelenusValidator.INSTANCE.validate(prop, value);

    if (key == null || value == null) {
      return null;
    }

    mutationsMap.put(key, value);
    return value;
  }

  public Object set(String key, Object value) {
    if (key == null || value == null) {
      return null;
    }

    mutationsMap.put(key, value);
    return value;
  }

  public void put(String key, Object value) {
    mutationsMap.put(key, value);
  }

  @SuppressWarnings("unchecked")
  public <T> T mutate(Getter<T> getter, T value) {
    return (T) mutate(this.<T>methodNameFor(getter), value);
  }

  public <T> T mutate(String key, T value) {
    Objects.requireNonNull(key);

    if (value != null) {
      if (entity != null) {
        T currentValue = this.<T>fetch(key);
        if (!value.equals(currentValue)) {
          mutationsMap.put(key, value);
          return value;
        }
      } else {
        mutationsMap.put(key, value);
      }
    }
    return null;
  }

  private <T> String methodNameFor(Getter<T> getter) {
    return MappingUtil.resolveMappingProperty(getter).getProperty().getPropertyName();
  }

  public <T> Object unset(Getter<T> getter) {
    return unset(methodNameFor(getter));
  }

  public Object unset(String key) {
    if (key != null) {
      Object value = mutationsMap.get(key);
      mutationsMap.put(key, null);
      return value;
    }
    return null;
  }

  public <T> boolean reset(Getter<T> getter, T desiredValue) {
    return this.<T>reset(this.<T>methodNameFor(getter), desiredValue);
  }

  private <T> T fetch(String key) {
    T value = (T) mutationsMap.get(key);
    if (value == null) {
      value = (T) valuesMap.get(key);
    }
    return value;
  }

  public <T> boolean reset(String key, T desiredValue) {
    if (key != null && desiredValue != null) {
      @SuppressWarnings("unchecked")
      T currentValue = (T) this.<T>fetch(key);
      if (currentValue == null || !currentValue.equals(desiredValue)) {
        set(key, desiredValue);
        return true;
      }
    }
    return false;
  }

  @Override
  public Map<String, Object> toMap() {
    return toMap(valuesMap);
  }

  public Map<String, Object> toMap(Map<String, Object> entityMap) {
    Map<String, Object> combined;
    if (entityMap != null && entityMap.size() > 0) {
      combined = new HashMap<String, Object>(entityMap.size());
      for (Map.Entry<String, Object> e : entityMap.entrySet()) {
        combined.put(e.getKey(), e.getValue());
      }
    } else {
      combined = new HashMap<String, Object>(mutationsMap.size());
    }
    for (String key : mutated()) {
      combined.put(key, mutationsMap.get(key));
    }
    return combined;
  }

  @Override
  public Set<String> mutated() {
    return mutationsMap.keySet();
  }

  @Override
  public Set<String> read() {
    return readSet;
  }

  @Override
  public String toString() {
    return mutationsMap.toString();
  }
}
