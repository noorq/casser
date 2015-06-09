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
package com.noorq.casser.mapping;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import javax.validation.Constraint;
import javax.validation.ConstraintValidator;

import com.noorq.casser.core.Casser;
import com.noorq.casser.core.Getter;
import com.noorq.casser.core.reflect.CasserPropertyNode;
import com.noorq.casser.core.reflect.DslExportable;
import com.noorq.casser.core.reflect.ListDsl;
import com.noorq.casser.core.reflect.MapDsl;
import com.noorq.casser.core.reflect.MapExportable;
import com.noorq.casser.core.reflect.ReflectionInstantiator;
import com.noorq.casser.core.reflect.SetDsl;
import com.noorq.casser.mapping.annotation.Index;
import com.noorq.casser.mapping.annotation.Table;
import com.noorq.casser.mapping.annotation.Tuple;
import com.noorq.casser.mapping.annotation.UDT;
import com.noorq.casser.support.CasserMappingException;
import com.noorq.casser.support.DslPropertyException;


public final class MappingUtil {

	@SuppressWarnings("unchecked")
	public static final ConstraintValidator<? extends Annotation, ?>[] EMPTY_VALIDATORS = new ConstraintValidator[0];
	
	private MappingUtil() {
	}
	
	public static ConstraintValidator<? extends Annotation, ?>[] getValidators(Method getterMethod) {

		List<ConstraintValidator<? extends Annotation, ?>> list = null;
		
		for (Annotation constraintAnnotation : getterMethod.getDeclaredAnnotations()) {
			
			Class<? extends Annotation> annotationType = constraintAnnotation.annotationType();
			
			Constraint constraint = annotationType.getDeclaredAnnotation(Constraint.class);
			
			if (constraint == null) {
				continue;
			}
			
			for (Class<? extends ConstraintValidator<?, ?>> clazz : constraint.validatedBy()) {

				ConstraintValidator<? extends Annotation, ?> validator = ReflectionInstantiator.instantiateClass(clazz);
				
				((ConstraintValidator) validator).initialize(constraintAnnotation);
				
				if (list == null) {
					list = new ArrayList<ConstraintValidator<? extends Annotation, ?>>();
				}
				
				list.add(validator);

			}
			
		}
		
		if (list == null) {
			return EMPTY_VALIDATORS;
		}
		else {
			return list.toArray(EMPTY_VALIDATORS);
		}
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

		return indexName != null ? Optional.of(new IdentityName(indexName, forceQuote)) : Optional.empty();
	}

	public static String getPropertyName(Method getter) {
		return getter.getName();
	}

	public static String getDefaultColumnName(Method getter) {
		return Casser.settings().getPropertyToColumnConverter()
				.apply(getPropertyName(getter));
	}

	public static IdentityName getUserDefinedTypeName(Class<?> iface, boolean required) {

		String userTypeName = null;
		boolean forceQuote = false;
		
		UDT userDefinedType = iface
				.getDeclaredAnnotation(UDT.class);

		if (userDefinedType != null) {
			
			userTypeName = userDefinedType.value();
			forceQuote = userDefinedType.forceQuote();
			
			if (userTypeName == null || userTypeName.isEmpty()) {
				userTypeName = getDefaultEntityName(iface);
			}
			
			return new IdentityName(userTypeName, forceQuote);

		} 
		
		if (required) {
			throw new CasserMappingException(
					"entity must have annotation @UserDefinedType " + iface);
		}

		return null;

	}
	
	public static boolean isTuple(Class<?> iface) {
		
		Tuple tuple = iface
				.getDeclaredAnnotation(Tuple.class);
		
		return tuple != null;
		
	}

	public static boolean isUDT(Class<?> iface) {
		
		UDT udt = iface
				.getDeclaredAnnotation(UDT.class);
		
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
			throw new CasserMappingException(
					"entity must have annotation @Table " + iface);
		}

		if (tableName == null || tableName.isEmpty()) {
			tableName = getDefaultEntityName(iface);
		}

		return new IdentityName(tableName, forceQuote);
	}

	public static String getDefaultEntityName(Class<?> iface) {
		return Casser.settings().getPropertyToColumnConverter()
				.apply(iface.getSimpleName());
	}

	public static Class<?> getMappingInterface(Object pojo) {

		Class<?> iface = null;

		if (pojo instanceof Class) {
			iface = (Class<?>) pojo;

			if (!iface.isInterface()) {
				throw new CasserMappingException("expected interface " + iface);
			}

		} else {
			Class<?>[] ifaces = pojo.getClass().getInterfaces();
			
			int len = ifaces.length;
			for (int i = 0; i != len; ++i) {
				
				iface = ifaces[0];
				
				if (MapExportable.class.isAssignableFrom(iface)) {
					continue;
				}
				
				if (iface.getDeclaredAnnotation(Table.class) != null ||
						iface.getDeclaredAnnotation(UDT.class) != null ||
						iface.getDeclaredAnnotation(Tuple.class) != null) {
					
					break;
					
				}
				
			}
			

		}

		if (iface == null) {
			throw new CasserMappingException("dsl interface not found for " + pojo);
		}
		
		return iface;

	}

	public static CasserPropertyNode resolveMappingProperty(
			Getter<?> getter) {

		try {
			Object childDsl = getter.get();
			
			if (childDsl instanceof DslExportable) {
				DslExportable e = (DslExportable) childDsl;
				return e.getParentDslCasserPropertyNode();
			}
			
			else if (childDsl instanceof MapDsl) {
				MapDsl mapDsl = (MapDsl) childDsl;
				return mapDsl.getParent();
			}

			else if (childDsl instanceof ListDsl) {
				ListDsl listDsl = (ListDsl) childDsl;
				return listDsl.getParent();
			}

			else if (childDsl instanceof SetDsl) {
				SetDsl setDsl = (SetDsl) childDsl;
				return setDsl.getParent();
			}

			throw new CasserMappingException(
					"getter must reference to the dsl object " + getter);
			
		} catch (DslPropertyException e) {
			return e.getPropertyNode();
		}

	}


}
