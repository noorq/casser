package casser.config;

import java.util.function.Function;

import casser.converter.CamelCaseToUnderscoreConverter;
import casser.converter.MethodNameToPropertyConverter;

public class DefaultCasserSettings implements CasserSettings {

	@Override
	public Function<String, String> getMethodNameToPropertyConverter() {
		return MethodNameToPropertyConverter.INSTANCE;
	}

	@Override
	public Function<String, String> getPropertyToColumnConverter() {
		return CamelCaseToUnderscoreConverter.INSTANCE;
	}

}
