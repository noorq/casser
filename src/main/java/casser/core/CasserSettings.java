package casser.core;

import java.util.function.Function;

public interface CasserSettings {

	Function<String, String> getMethodNameToPropertyConverter();

	Function<String, String> getPropertyToColumnConverter();

}
