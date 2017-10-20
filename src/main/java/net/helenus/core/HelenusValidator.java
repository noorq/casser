/*
 *      Copyright (C) 2015 The Helenus Authors
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
package net.helenus.core;

import java.lang.annotation.Annotation;

import javax.validation.ConstraintValidator;

import net.helenus.mapping.HelenusProperty;
import net.helenus.support.HelenusException;
import net.helenus.support.HelenusMappingException;

public enum HelenusValidator implements PropertyValueValidator {
	INSTANCE;

	public void validate(HelenusProperty prop, Object value) {

		for (ConstraintValidator<? extends Annotation, ?> validator : prop.getValidators()) {

			ConstraintValidator typeless = (ConstraintValidator) validator;

			boolean valid = false;

			try {
				valid = typeless.isValid(value, null);
			} catch (ClassCastException e) {
				throw new HelenusMappingException("validator was used for wrong type '" + value + "' in " + prop, e);
			}

			if (!valid) {
				throw new HelenusException("wrong value '" + value + "' for " + prop);
			}
		}
	}
}
