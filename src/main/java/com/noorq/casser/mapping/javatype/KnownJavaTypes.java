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
package com.noorq.casser.mapping.javatype;

import java.util.Optional;

import com.google.common.collect.ImmutableMap;

public final class KnownJavaTypes {

	private static final ImmutableMap<Class<?>, AbstractJavaType> knownTypes;
	
	static {
		
		ImmutableMap.Builder<Class<?>, AbstractJavaType> builder = ImmutableMap.builder();

		add(builder, new ByteBufferJavaType());
		add(builder, new ByteArrayJavaType());
		add(builder, new DateJavaType());
		add(builder, new LongJavaType());
		add(builder, new StringJavaType());
		
		knownTypes = builder.build();
		
	}
	
	private static void add(ImmutableMap.Builder<Class<?>, AbstractJavaType> builder, AbstractJavaType jt) {
		
		builder.put(jt.getJavaClass(), jt);
		
		Optional<Class<?>> primitiveJavaClass = jt.getPrimitiveJavaClass();
		if (primitiveJavaClass.isPresent()) {
			builder.put(primitiveJavaClass.get(), jt);
		}
		
	}
	
	private KnownJavaTypes() {
	}
	
	public static AbstractJavaType findJavaType(Class<?> javaClass) {
		return knownTypes.get(javaClass);
	}
	
}
