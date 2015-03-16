package casser.core;

import java.util.function.Function;

import com.google.common.base.CaseFormat;

public enum CamelCaseToUnderscoreConverter implements Function<String, String> {

	INSTANCE;

	@Override
	public String apply(String source) {
		return CaseFormat.UPPER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, source);
	}

}