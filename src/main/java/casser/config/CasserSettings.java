package casser.config;

import java.lang.reflect.Method;
import java.util.function.Function;

public interface CasserSettings {

	Function<String, String> getMethodNameToPropertyConverter();

	Function<String, String> getPropertyToColumnConverter();
	
	Function<Method, Boolean> getGetterMethodDetector();
	
	Function<Method, Boolean> getSetterMethodDetector();
	

}
