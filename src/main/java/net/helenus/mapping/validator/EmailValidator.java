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
package net.helenus.mapping.validator;

import java.net.IDN;
import java.util.regex.Pattern;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;

import net.helenus.mapping.annotation.Constraints.Email;

public final class EmailValidator implements ConstraintValidator<Email, CharSequence> {

	static final String ATOM = "[a-z0-9!#$%&'*+/=?^_`{|}~-]";
	static final String DOMAIN = "(" + ATOM + "+(\\." + ATOM + "+)*";
	static final String IP_DOMAIN = "\\[[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\]";

	static final String PATTERN = "^" + ATOM + "+(\\." + ATOM + "+)*@" + DOMAIN + "|" + IP_DOMAIN + ")$";

	private static final Pattern pattern = Pattern.compile(PATTERN, Pattern.CASE_INSENSITIVE);

	@Override
	public void initialize(Email constraintAnnotation) {
	}

	@Override
	public boolean isValid(CharSequence value, ConstraintValidatorContext context) {

		if (value == null) {
			return true;
		}

		String asciiString = IDN.toASCII(value.toString());

		return pattern.matcher(asciiString).matches();
	}

}
