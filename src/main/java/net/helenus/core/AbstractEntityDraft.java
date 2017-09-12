package net.helenus.core;

import java.util.*;

import com.google.common.primitives.Primitives;

import net.helenus.core.reflect.DefaultPrimitiveTypes;
import net.helenus.core.reflect.Drafted;
import net.helenus.core.reflect.MapExportable;
import net.helenus.mapping.MappingUtil;


public abstract class AbstractEntityDraft<E> implements Drafted<E> {

    private final Map<String, Object> backingMap = new HashMap<String, Object>();
    private final Set<String> mutatedSet = new HashSet<String>();
    private final MapExportable entity;
    private final Map<String, Object> entityMap;


    public AbstractEntityDraft(MapExportable entity) {
        this.entity = entity;
        this.entityMap = entity != null ? entity.toMap() : new HashMap<String, Object>();
    }

    public abstract Class<E> getEntityClass();

    public E build() { return Helenus.map(getEntityClass(), toMap()); }

    protected <T> T get(String key, Class<?> returnType) {
        T value = (T) entityMap.get(key);

        if (value == null) {

            if (Primitives.allPrimitiveTypes().contains(returnType)) {

                DefaultPrimitiveTypes type = DefaultPrimitiveTypes.lookup(returnType);
                if (type == null) {
                    throw new RuntimeException("unknown primitive type " + returnType);
                }

                return (T) type.getDefaultValue();
            }
        }

        return value;
    }

    protected Object set(String key, Object value) {

        if (key == null || value == null) {
            return null;
        }

        backingMap.put(key, value);
        mutatedSet.add(key);
        return value;
    }

    protected Object mutate(String key, Object value) {
        Objects.requireNonNull(key);

        if (value == null) {
            return null;
        }

        if (entity != null) {
            Map<String, Object> map = entity.toMap();

            if (map.containsKey(key) && !value.equals(map.get(key))) {
                backingMap.put(key, value);
                mutatedSet.add(key);
                return value;
            }

            return map.get(key);
        } else {
            backingMap.put(key, value);
            mutatedSet.add(key);

            return null;
        }
    }

    private String methodNameFor(Getter<?> getter) {
        return MappingUtil.resolveMappingProperty(getter)
                .getProperty()
                .getPropertyName();
    }

    public Object unset(Getter<?> getter) {
        return unset(methodNameFor(getter));
    }

    public Object unset(String key) {
        if (key != null) {
            Object value = backingMap.get(key);
            backingMap.put(key, null);
            mutatedSet.add(key);
            return value;
        }
        return null;
    }

    public <T> boolean reset(Getter<?> getter, T desiredValue) {
        return this.<T>reset(methodNameFor(getter), desiredValue);
    }

    public <T> boolean reset(String key, T desiredValue) {
        if (key != null && desiredValue != null) {
            @SuppressWarnings("unchecked")
            T currentValue = (T) backingMap.get(key);
            if (currentValue != null && !currentValue.equals(desiredValue)) {
                return set(key, desiredValue) != null;
            }
        }
        return false;
    }

    @Override
    public Map<String, Object> toMap() {
        return toMap(entityMap);
    }

    public Map<String, Object> toMap(Map<String, Object>entityMap) {
        Map<String, Object> combined;
        if (entityMap != null && entityMap.size() > 0) {
            combined = new HashMap<String, Object>(entityMap.size());
            for (String key : entityMap.keySet()) {
                combined.put(key, entityMap.get(key));
            }
        } else {
            combined = new HashMap<String, Object>(backingMap.size());
        }
        for (String key : mutatedSet) {
            combined.put(key, backingMap.get(key));
        }
        return combined;
    }

    @Override
    public Set<String> mutated() {
        return mutatedSet;
    }

    @Override
    public String toString() {
        return backingMap.toString();
    }

}
