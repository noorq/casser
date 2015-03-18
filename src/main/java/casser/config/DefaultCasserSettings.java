package casser.config;

import java.lang.reflect.Method;
import java.util.function.Function;

import casser.mapping.converter.CamelCaseToUnderscoreConverter;
import casser.mapping.converter.MethodNameToPropertyConverter;

public class DefaultCasserSettings implements CasserSettings {

	@Override
	public Function<String, String> getMethodNameToPropertyConverter() {
		return MethodNameToPropertyConverter.INSTANCE;
	}

	@Override
	public Function<String, String> getPropertyToColumnConverter() {
		return CamelCaseToUnderscoreConverter.INSTANCE;
	}

	@Override
	public Function<Method, Boolean> getGetterMethodDetector() {
		return GetterMethodDetector.INSTANCE;
	}

	@Override
	public Function<Method, Boolean> getSetterMethodDetector() {
		return SetterMethodDetector.INSTANCE;
	}

}
