package casser.config;

import java.lang.reflect.Method;
import java.util.function.Function;

public enum GetterMethodDetector implements Function<Method, Boolean> {

	INSTANCE;
	
	@Override
	public Boolean apply(Method method) {
		
		if (method == null) {
			throw new IllegalArgumentException("empty parameter");
		}
		
		if (method.getParameterCount() != 0 || method.getReturnType() == void.class) {
			return false;
		}
		
		String methodName = method.getName();
		
	    return methodName.startsWith("get") || methodName.startsWith("is") || methodName.startsWith("has");
	    
	}

}
