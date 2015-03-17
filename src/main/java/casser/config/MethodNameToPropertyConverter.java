package casser.config;

import java.util.function.Function;

public enum MethodNameToPropertyConverter implements Function<String, String> {

	INSTANCE;

	@Override
	public String apply(String source) {
		
		if (source == null) {
			throw new IllegalArgumentException("empty parameter");
		}
		
		if (source.startsWith("get")) {
			return source.substring(3);
		}

		if (source.startsWith("set")) {
			return source.substring(3);
		}

		if (source.startsWith("has")) {
			return source.substring(3);
		}

		if (source.startsWith("is")) {
			return source.substring(2);
		}

		return source;
	}

}