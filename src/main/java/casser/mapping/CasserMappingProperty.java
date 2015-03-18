package casser.mapping;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import casser.core.Casser;
import casser.support.CasserMappingException;

import com.datastax.driver.core.DataType;

public class CasserMappingProperty<E> implements CasserProperty<E> {

	private final CasserMappingEntity<E> entity; 
	
	private final Method getterMethod;
	private Method setterMethod;
	
	private final String propertyName;
	
	private boolean keyInfo = false;
	private boolean isPartitionKey = false;
	private boolean isClusteringColumn = false;
	private int ordinal = 0;
	private Ordering ordering = null;
	
	private Class<?> javaType = null;
	
	private boolean typeInfo = false;
	private DataType dataType = null;
	
	private String columnName;
	
	public CasserMappingProperty(CasserMappingEntity<E> entity, Method getter) {
		this.entity = entity;
		this.getterMethod = getter;
		this.propertyName = getPropertyName(getter);
	}
	
	@Override
	public CasserMappingEntity<E> getEntity() {
		return entity;
	}
	
	private void ensureKeyInfo() {
		if (!keyInfo) {
			PartitionKey partitionKey = getterMethod.getDeclaredAnnotation(PartitionKey.class);
			
			if (partitionKey != null) {
				isPartitionKey = true;
				ordinal = partitionKey.ordinal();
			}
			
			ClusteringColumn clusteringColumnn = getterMethod.getDeclaredAnnotation(ClusteringColumn.class);
			
			if (clusteringColumnn != null) {
				
				if (isPartitionKey) {
					throw new CasserMappingException("property can be annotated only by single column type " + getPropertyName() + " in " + this.entity.getEntityInterface());
				}
				
				isClusteringColumn = true;
				ordinal = clusteringColumnn.ordinal();
				ordering = clusteringColumnn.ordering();
			}
			
			keyInfo = true;
		}
	}

    @Override
	public Class<?> getJavaType() {
		if (javaType == null) {
			javaType = getterMethod.getReturnType();
		}
		return javaType;
	}
	
	@Override
	public DataType getDataType() {

		if (!typeInfo) {
			dataType = resolveDataType();
			typeInfo = true;
		}
		
		return dataType;
	}

	@Override
	public boolean isPartitionKey() {
		ensureKeyInfo();
		return isPartitionKey;
	}

	@Override
	public boolean isClusteringColumn() {
		ensureKeyInfo();
		return isClusteringColumn;
	}

	@Override
	public int getOrdinal() {
		ensureKeyInfo();
		return ordinal;
	}

	@Override
	public Ordering getOrdering() {
		ensureKeyInfo();
		return ordering;
	}

	@Override
	public String getColumnName() {
		
		if (columnName == null) {
			
			Column column = getterMethod.getDeclaredAnnotation(Column.class);
			if (column != null) {
				columnName = column.value();
			}

			PartitionKey partitionKey = getterMethod.getDeclaredAnnotation(PartitionKey.class);
			if (partitionKey != null) {
				
				if (columnName != null) {
					throw new CasserMappingException("property can be annotated only by single column type " + getPropertyName() + " in " + this.entity.getEntityInterface());
				}
				
				columnName = partitionKey.value();
			}
			
			ClusteringColumn clusteringColumn = getterMethod.getDeclaredAnnotation(ClusteringColumn.class);
			if (clusteringColumn != null) {
				
				if (columnName != null) {
					throw new CasserMappingException("property can be annotated only by single column type " + getPropertyName() + " in " + this.entity.getEntityInterface());
				}
				
				columnName = clusteringColumn.value();
			}
			
			if (columnName == null || columnName.isEmpty()) {
				columnName = getDefaultColumnName();
			}
			
		}
		
		return columnName;
	}
	
	private String getDefaultColumnName() {
		return Casser.settings().getPropertyToColumnConverter().apply(propertyName);
	}

	public void setSetterMethod(Method setterMethod) {
		this.setterMethod = setterMethod;
	}

	public String getPropertyName() {
		return propertyName;
	}

	public Method getGetterMethod() {
		return getterMethod;
	}

	public Method getSetterMethod() {
		return setterMethod;
	}
	
	
	public static String getPropertyName(Method m) {
		
		String propertyName = Casser.settings().getMethodNameToPropertyConverter().apply(m.getName());
		
		return propertyName;
	}
	
	private DataType resolveDataType() {
		
		Class<?> propertyType = getJavaType();

		Qualify annotation = getterMethod.getDeclaredAnnotation(Qualify.class);
		if (annotation != null && annotation.type() != null) {
			return qualifyAnnotatedType(annotation);
		}

		if (Enum.class.isAssignableFrom(propertyType)) {
			return DataType.text();
		}

		if (isMap()) {
			Type[] args = getTypeArguments();
			ensureTypeArguments(args.length, 2);

			return DataType.map(autodetectPrimitiveType(args[0]),
					autodetectPrimitiveType(args[1]));
		}

		if (isCollectionLike()) {
			Type[] args = getTypeArguments();
			ensureTypeArguments(args.length, 1);

			if (Set.class.isAssignableFrom(propertyType)) {

				return DataType.set(autodetectPrimitiveType(args[0]));

			} else if (List.class.isAssignableFrom(propertyType)) {

				return DataType.list(autodetectPrimitiveType(args[0]));

			}
		}

		DataType dataType = SimpleDataTypes.getDataTypeByJavaClass(propertyType);
		if (dataType == null) {
			throw new CasserMappingException(
					"only primitive types and Set,List,Map collections are allowed, unknown type for property '" + this.getPropertyName()
							+ "' type is '" + this.getJavaType() + "' in the entity " + this.entity.getEntityInterface());
		}

		return dataType;
	}
	
	private DataType qualifyAnnotatedType(Qualify annotation) {
		DataType.Name type = annotation.type();
		if (type.isCollection()) {
			switch (type) {
			case MAP:
				ensureTypeArguments(annotation.typeArguments().length, 2);
				return DataType.map(resolvePrimitiveType(annotation.typeArguments()[0]),
						resolvePrimitiveType(annotation.typeArguments()[1]));
			case LIST:
				ensureTypeArguments(annotation.typeArguments().length, 1);
				return DataType.list(resolvePrimitiveType(annotation.typeArguments()[0]));
			case SET:
				ensureTypeArguments(annotation.typeArguments().length, 1);
				return DataType.set(resolvePrimitiveType(annotation.typeArguments()[0]));
			default:
				throw new CasserMappingException("unknown collection DataType for property '" + this.getPropertyName()
						+ "' type is '" + this.getJavaType() + "' in the entity " + this.entity.getEntityInterface());
			}
		} else {
			return SimpleDataTypes.getDataTypeByName(type);
		}
	}
	
	DataType resolvePrimitiveType(DataType.Name typeName) {
		DataType dataType = SimpleDataTypes.getDataTypeByName(typeName);
		if (dataType == null) {
			throw new CasserMappingException(
					"only primitive types are allowed inside collections for the property  '" + this.getPropertyName() + "' type is '"
							+ this.getJavaType() + "' in the entity " + this.entity.getEntityInterface());
		}
		return dataType;
	}

	DataType autodetectPrimitiveType(Type javaType) {
		DataType dataType = SimpleDataTypes.getDataTypeByJavaClass(javaType);
		if (dataType == null) {
			throw new CasserMappingException(
					"only primitive types are allowed inside collections for the property  '" + this.getPropertyName() + "' type is '"
							+ this.getJavaType() + "' in the entity " + this.entity.getEntityInterface());
		}
		return dataType;
	}
	
	void ensureTypeArguments(int args, int expected) {
		if (args != expected) {
			throw new CasserMappingException("expected " + expected + " of typed arguments for the property  '"
					+ this.getPropertyName() + "' type is '" + this.getJavaType() + "' in the entity " + this.entity.getEntityInterface());
		}
	}
	
	boolean isMap() {
		return Map.class.isAssignableFrom(getJavaType());
	}
	
	boolean isCollectionLike() {

		Class<?> rawType = getJavaType();

		if (rawType.isArray() || Iterable.class.equals(rawType)) {
			return true;
		}

		return Collection.class.isAssignableFrom(rawType);
	}
	
	Type[] getTypeArguments() {
		
		Type javaType = (Type) getJavaType();
		
		if (javaType instanceof ParameterizedType) {
		
			ParameterizedType type = (ParameterizedType) javaType;
		
			return type.getActualTypeArguments();
		}
		
		return new Type[] {};
	}

}
