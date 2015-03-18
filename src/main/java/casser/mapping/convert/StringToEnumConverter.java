package casser.mapping.convert;

import java.util.function.Function;

public class StringToEnumConverter implements Function<String, Object> {

	private final Class<? extends Enum> enumClass;

	public StringToEnumConverter(Class<?> enumClass) {
		this.enumClass = (Class<? extends Enum>) enumClass;
	}

	@Override
	public Object apply(String source) {
		return Enum.valueOf(enumClass, source);
	}

}
