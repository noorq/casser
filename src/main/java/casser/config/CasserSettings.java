/*
 *      Copyright (C) 2015 Noorq Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package casser.config;

import java.lang.reflect.Method;
import java.util.function.Function;

public interface CasserSettings {

	Function<String, String> getMethodNameToPropertyConverter();

	Function<String, String> getPropertyToColumnConverter();
	
	Function<Method, Boolean> getGetterMethodDetector();
	
	Function<Method, Boolean> getSetterMethodDetector();
	

}
