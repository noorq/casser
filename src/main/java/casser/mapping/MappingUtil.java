/*
 *      Copyright (C) 2015 Noorq, Inc.
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
package casser.mapping;

import java.lang.reflect.Method;

import casser.core.Casser;
import casser.core.Getter;
import casser.support.CasserMappingException;
import casser.support.DslPropertyException;

public final class MappingUtil {

	private MappingUtil() {
	}

	public static String getColumnName(Method getterMethod) {

		String columnName = null;

		Column column = getterMethod.getDeclaredAnnotation(Column.class);
		if (column != null) {
			columnName = column.value();
			if (column.forceQuote()) {
				columnName = CqlUtil.forceQuote(columnName);
			}
		}

		PartitionKey partitionKey = getterMethod
				.getDeclaredAnnotation(PartitionKey.class);
		if (partitionKey != null) {

			if (columnName != null) {
				throw new CasserMappingException(
						"property can be annotated only by single column type "
								+ getterMethod);
			}

			columnName = partitionKey.value();
			if (partitionKey.forceQuote()) {
				columnName = CqlUtil.forceQuote(columnName);
			}
		}

		ClusteringColumn clusteringColumn = getterMethod
				.getDeclaredAnnotation(ClusteringColumn.class);
		if (clusteringColumn != null) {

			if (columnName != null) {
				throw new CasserMappingException(
						"property can be annotated only by single column type "
								+ getterMethod);
			}

			columnName = clusteringColumn.value();
			if (clusteringColumn.forceQuote()) {
				columnName = CqlUtil.forceQuote(columnName);
			}
		}

		if (columnName == null || columnName.isEmpty()) {
			columnName = getDefaultColumnName(getterMethod);
		}

		return columnName;
	}

	public static String getPropertyName(Method getter) {
		return getter.getName();
	}

	public static String getDefaultColumnName(Method getter) {
		return Casser.settings().getPropertyToColumnConverter()
				.apply(getPropertyName(getter));
	}

	public static String getUserDefinedTypeName(Class<?> iface, boolean required) {

		String userTypeName = null;

		UserDefinedType userDefinedType = iface
				.getDeclaredAnnotation(UserDefinedType.class);

		if (userDefinedType != null) {
			userTypeName = userDefinedType.value();

			if (userDefinedType.forceQuote()) {
				userTypeName = CqlUtil.forceQuote(userTypeName);
			}

		} else if (required) {
			throw new CasserMappingException(
					"entity must have annotation @UserDefinedType " + iface);
		}

		if (userTypeName == null || userTypeName.isEmpty()) {
			userTypeName = getDefaultName(iface);
		}

		return userTypeName;
	}

	public static String getTableName(Class<?> iface, boolean required) {

		String tableName = null;

		Table table = iface.getDeclaredAnnotation(Table.class);

		if (table != null) {
			tableName = table.value();

			if (table.forceQuote()) {
				tableName = CqlUtil.forceQuote(tableName);
			}

		} else if (required) {
			throw new CasserMappingException(
					"entity must have annotation @Table " + iface);
		}

		if (tableName == null || tableName.isEmpty()) {
			tableName = getDefaultName(iface);
		}

		return tableName;
	}

	public static String getDefaultName(Class<?> iface) {
		return Casser.settings().getPropertyToColumnConverter()
				.apply(iface.getSimpleName());
	}

	public static Class<?> getMappingInterface(Object entity) {

		Class<?> iface = null;

		if (entity instanceof Class) {
			iface = (Class<?>) entity;

			if (!iface.isInterface()) {
				throw new CasserMappingException("expected interface " + iface);
			}

		} else {
			Class<?>[] ifaces = entity.getClass().getInterfaces();
			
			int len = ifaces.length;
			for (int i = 0; i != len; ++i) {
				
				iface = ifaces[0];
				
				if (MapExportable.class.isAssignableFrom(iface)) {
					continue;
				}
				
				if (iface.getDeclaredAnnotation(Table.class) != null ||
						iface.getDeclaredAnnotation(UserDefinedType.class) != null) {
					
					break;
					
				}
				
			}
			

		}

		if (iface == null) {
			throw new CasserMappingException("dsl interface not found for " + entity);
		}
		
		return iface;

	}

	public static CasserMappingProperty resolveMappingProperty(
			Getter<?> getter) {

		try {
			getter.get();
			throw new CasserMappingException(
					"getter must reference to a dsl object " + getter);
		} catch (DslPropertyException e) {
			return (CasserMappingProperty) e.getProperty();
		}

	}


}
