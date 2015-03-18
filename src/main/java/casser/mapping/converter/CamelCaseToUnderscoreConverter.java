package casser.mapping.converter;

import java.util.function.Function;

import com.google.common.base.CaseFormat;

public enum CamelCaseToUnderscoreConverter implements Function<String, String> {

	INSTANCE;

	@Override
	public String apply(String source) {
		
		if (source == null) {
			throw new IllegalArgumentException("empty parameter");
		}
		
		return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, source);
	}

}