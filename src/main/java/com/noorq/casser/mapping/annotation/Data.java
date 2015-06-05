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
package com.noorq.casser.mapping.annotation;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Data annotations are using for data integrity. Annotations helps to build simple data model without complex operations in repositories. 
 * All of them are placing for particular columns under the methods in model DSL.
 * 
 * Some of them are using for validation only, like @NotNull or @Length 
 * Some of them, like @LowerCase and @UpperCase are doing actual modifications under the data and working as preprocessors.
 * 
 * All of them does not have effect on selects and data retrieval operations.
 * 
 * Support types: 
 * - @NotNull supports any @Object type
 * - All annotations support @String type
 * 
 * @author Albert Shift
 *
 */

public final class Data {

	private Data() {
	}

	/**
	 *  NotNull annotation is using to check that value is not null before storing it 
	 *  
	 *  It does not check on selects and data retrieval operations
     *
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface NotNull {

	}
	
	/**
	 *  Length annotation is using to ensure that value has exact length before storing it
	 *
	 *  It does not have effect on selects and data retrieval operations
	 *
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Length {

		int value();
		
	}
	
	/**
	 *  MaxLength annotation is using to ensure that value has length less or equal to some threshold before storing it
	 *
	 *  It does not have effect on selects and data retrieval operations
	 *
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface MaxLength {

		int value();
		
	}
	
	/**
	 *  MinLength annotation is using to ensure that value has length greater or equal to some threshold before storing it
	 *
	 *  It does not have effect on selects and data retrieval operations
	 *
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface MinLength {

		int value();
		
	}
	
	/**
	 *  LowerCase annotation is the preprocessor annotation that converts String value to lower case if it is not null
	 *
	 *  It does not have effect on selects and data retrieval operations
	 *
	 */		
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface LowerCase {

	}

	/**
	 *  UpperCase annotation is the preprocessor annotation that converts String value to upper case if it is not null
	 *
	 *  It does not have effect on selects and data retrieval operations
	 *
	 */	
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UpperCase {

	}
	
}
