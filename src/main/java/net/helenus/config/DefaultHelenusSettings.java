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
package net.helenus.config;

import java.lang.reflect.Method;
import java.util.function.Function;

import net.helenus.core.DslInstantiator;
import net.helenus.core.MapperInstantiator;
import net.helenus.core.reflect.ReflectionDslInstantiator;
import net.helenus.core.reflect.ReflectionMapperInstantiator;
import net.helenus.mapping.convert.CamelCaseToUnderscoreConverter;

public class DefaultHelenusSettings implements HelenusSettings {

	@Override
	public Function<String, String> getPropertyToColumnConverter() {
		return CamelCaseToUnderscoreConverter.INSTANCE;
	}

	@Override
	public Function<Method, Boolean> getGetterMethodDetector() {
		return GetterMethodDetector.INSTANCE;
	}

	@Override
	public DslInstantiator getDslInstantiator() {
		return ReflectionDslInstantiator.INSTANCE;
	}

	@Override
	public MapperInstantiator getMapperInstantiator() {
		return ReflectionMapperInstantiator.INSTANCE;
	}
}
