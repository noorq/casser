package casser.config;

import java.lang.reflect.Method;
import java.util.function.Function;

public enum SetterMethodDetector implements Function<Method, Boolean> {

	INSTANCE;
	
	@Override
	public Boolean apply(Method method) {
		
		if (method == null) {
			throw new IllegalArgumentException("empty parameter");
		}
		
		if (method.getParameterCount() != 1 || method.getReturnType() != void.class) {
			return false;
		}
		
		return method.getName().startsWith("set");
	}

}
