package net.helenus.core;

import java.io.Serializable;
import java.util.*;

import org.apache.commons.lang3.SerializationUtils;

import com.google.common.primitives.Primitives;

import net.helenus.core.reflect.DefaultPrimitiveTypes;
import net.helenus.core.reflect.Drafted;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.MappingUtil;

public abstract class AbstractEntityDraft<E> implements Drafted<E> {

	private final Map<String, Object> backingMap = new HashMap<String, Object>();
	private final MapExportable entity;
	private final Map<String, Object> entityMap;

	public AbstractEntityDraft(MapExportable entity) {
		this.entity = entity;
		this.entityMap = entity != null ? entity.toMap() : new HashMap<String, Object>();
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
		T value = (T) backingMap.get(key);

		if (value == null) {
			value = (T) entityMap.get(key);
			if (value == null) {

				if (Primitives.allPrimitiveTypes().contains(returnType)) {

					DefaultPrimitiveTypes type = DefaultPrimitiveTypes.lookup(returnType);
					if (type == null) {
						throw new RuntimeException("unknown primitive type " + returnType);
					}

					return (T) type.getDefaultValue();
				}
			} else {
				// Collections fetched from the entityMap
				if (value instanceof Collection) {
					try {
						value = MappingUtil.<T>clone(value);
					} catch (CloneNotSupportedException e) {
						// TODO(gburd): deep?shallow? copy of List, Map, Set to a mutable collection.
						value = (T) SerializationUtils.<Serializable>clone((Serializable) value);
					}
				}
			}
		}

		return value;
	}

	public <T> Object set(Getter<T> getter, Object value) {
		return set(this.<T>methodNameFor(getter), value);
	}

	public Object set(String key, Object value) {
		if (key == null || value == null) {
			return null;
		}

		backingMap.put(key, value);
		return value;
	}

	@SuppressWarnings("unchecked")
	public <T> T mutate(Getter<T> getter, T value) {
		return (T) mutate(this.<T>methodNameFor(getter), value);
	}

	public Object mutate(String key, Object value) {
		Objects.requireNonNull(key);

		if (value == null) {
			return null;
		}

		if (entity != null) {
			Map<String, Object> map = entity.toMap();

			if (map.containsKey(key) && !value.equals(map.get(key))) {
				backingMap.put(key, value);
				return value;
			}

			return map.get(key);
		} else {
			backingMap.put(key, value);

			return null;
		}
	}

	private <T> String methodNameFor(Getter<T> getter) {
		return MappingUtil.resolveMappingProperty(getter).getProperty().getPropertyName();
	}

	public <T> Object unset(Getter<T> getter) {
		return unset(methodNameFor(getter));
	}

	public Object unset(String key) {
		if (key != null) {
			Object value = backingMap.get(key);
			backingMap.put(key, null);
			return value;
		}
		return null;
	}

	public <T> boolean reset(Getter<T> getter, T desiredValue) {
		return this.<T>reset(this.<T>methodNameFor(getter), desiredValue);
	}

	public <T> boolean reset(String key, T desiredValue) {
		if (key != null && desiredValue != null) {
			@SuppressWarnings("unchecked")
			T currentValue = (T) backingMap.get(key);
			if (currentValue == null || !currentValue.equals(desiredValue)) {
				set(key, desiredValue);
				return true;
			}
		}
		return false;
	}

	@Override
	public Map<String, Object> toMap() {
		return toMap(entityMap);
	}

	public Map<String, Object> toMap(Map<String, Object> entityMap) {
		Map<String, Object> combined;
		if (entityMap != null && entityMap.size() > 0) {
			combined = new HashMap<String, Object>(entityMap.size());
			for (String key : entityMap.keySet()) {
				combined.put(key, entityMap.get(key));
			}
		} else {
			combined = new HashMap<String, Object>(backingMap.size());
		}
		for (String key : mutated()) {
			combined.put(key, backingMap.get(key));
		}
		return combined;
	}

	@Override
	public Set<String> mutated() {
		return backingMap.keySet();
	}

	@Override
	public String toString() {
		return backingMap.toString();
	}
}
