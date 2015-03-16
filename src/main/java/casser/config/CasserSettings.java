package casser.config;

import java.util.function.Function;

public interface CasserSettings {

	Function<String, String> getMethodNameToPropertyConverter();

	Function<String, String> getPropertyToColumnConverter();

}
