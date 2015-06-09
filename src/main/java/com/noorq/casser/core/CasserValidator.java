/*
 *      Copyright (C) 2015 Noorq, Inc.
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
package com.noorq.casser.core;

import java.lang.annotation.Annotation;

import javax.validation.ConstraintValidator;

import com.noorq.casser.mapping.CasserProperty;
import com.noorq.casser.support.CasserException;
import com.noorq.casser.support.CasserMappingException;

public final class CasserValidator {

	private CasserValidator() {
		
	}
	
	public static void validate(CasserProperty prop, Object value) {
		
		for (ConstraintValidator<? extends Annotation, ?> validator : prop.getValidators()) {
			
			ConstraintValidator typeless = (ConstraintValidator) validator;
			
			
			boolean valid = false;
			
			try {
				valid = typeless.isValid(value, null);
			}
			catch(ClassCastException e) {
				throw new CasserMappingException("validator was used for wrong type '" + value + "' in " + prop, e);
			}
			
			if (!valid) {
				throw new CasserException("wrong value type '" + value + "' for " + prop);
			}
		}
		
	}
	
}
