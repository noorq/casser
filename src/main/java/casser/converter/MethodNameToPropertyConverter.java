package casser.converter;

import java.util.function.Function;

public enum MethodNameToPropertyConverter implements Function<String, String> {

	INSTANCE;

	@Override
	public String apply(String source) {
		
		if (source.startsWith("get")) {
			return source.substring(3);
		}

		if (source.startsWith("set")) {
			return source.substring(3);
		}

		if (source.startsWith("is")) {
			return source.substring(2);
		}

		return source;
	}

}