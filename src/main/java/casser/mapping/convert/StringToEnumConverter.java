package casser.mapping.convert;

import java.util.function.Function;

public class StringToEnumConverter implements Function<String, Enum> {

	private final Class<? extends Enum> enumClass;

	public StringToEnumConverter(Class<?> enumClass) {
		this.enumClass = (Class<? extends Enum>) enumClass;
	}

	@Override
	public Enum apply(String source) {
		return Enum.valueOf(enumClass, source);
	}

}
