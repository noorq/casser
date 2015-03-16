package casser.core;

import java.util.function.Function;

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
