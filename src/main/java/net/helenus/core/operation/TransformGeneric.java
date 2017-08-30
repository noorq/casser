package net.helenus.core.operation;

import java.util.*;
import java.util.function.Function;

import net.helenus.core.AbstractSessionOperations;
import net.helenus.core.Helenus;
import net.helenus.core.reflect.DefaultPrimitiveTypes;
import net.helenus.core.reflect.HelenusPropertyNode;
import net.helenus.mapping.HelenusProperty;
import net.helenus.mapping.value.BeanColumnValueProvider;
import net.helenus.support.Fun;
import net.helenus.support.HelenusException;

public enum TransformGeneric {
    INSTANCE;

    public <T> T transform(AbstractSessionOperations sessionOps, T pojo, Class<?> mappingInterface, List<Fun.Tuple2<HelenusPropertyNode, Object>> assignments, Collection<HelenusProperty> properties) {
        if (assignments.size() > 0) {
            Map<String, Object> backingMap = new HashMap<String, Object>(properties.size());

            // First, add all the inserted values into our new map.
            assignments.forEach(t -> backingMap.put(t._1.getProperty().getPropertyName(), t._2));

            // Then, fill in all the rest of the properties.
            for (HelenusProperty prop : properties) {
                String key = prop.getPropertyName();
                if (backingMap.containsKey(key)) {
                    // Some values man need to be converted (e.g. from String to Enum).  This is done
                    // within the BeanColumnValueProvider below.
                    Optional<Function<Object, Object>> converter =
                            prop.getReadConverter(sessionOps.getSessionRepository());
                    if (converter.isPresent()) {
                        backingMap.put(key, converter.get().apply(backingMap.get(key)));
                    }
                } else {
                    // If we started this operation with an instance of this type, use values from that.
                    if (pojo != null) {
                        backingMap.put(key, BeanColumnValueProvider.INSTANCE.getColumnValue(pojo, -1, prop));
                    } else {
                        // Otherwise we'll use default values for the property type if available.
                        Class<?> propType = prop.getJavaType();
                        if (propType.isPrimitive()) {
                            DefaultPrimitiveTypes type = DefaultPrimitiveTypes.lookup(propType);
                            if (type == null) {
                                throw new HelenusException("unknown primitive type " + propType);
                            }
                            backingMap.put(key, type.getDefaultValue());
                        }
                    }
                }
            }

            // Lastly, create a new proxy object for the entity and return the new instance.
            return (T) Helenus.map(mappingInterface, backingMap);
        }
        return (T) pojo;
    }
}
