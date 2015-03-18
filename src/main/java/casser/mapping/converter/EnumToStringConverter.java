package casser.mapping.converter;

import java.util.function.Function;

/**
 * Enum to String Converter
 * 
 */

public enum EnumToStringConverter implements Function<Enum, String> {

	INSTANCE;

	@Override
	public String apply(Enum source) {
		return source.name();
	}

}
