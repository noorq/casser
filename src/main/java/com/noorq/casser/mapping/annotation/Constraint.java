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
 * Constraint annotations are using for data integrity mostly for @java.lang.String types.  
 * The place of the annotation is the particular method in model interface.
 * 
 * All of them does not have effect on selects and data retrieval operations.
 * 
 * Support types: 
 * - @NotNull supports any @java.lang.Object type
 * - All annotations support @java.lang.String type
 * 
 * @author Albert Shift
 *
 */

public final class Constraint {

	private Constraint() {
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
	 *  NotEmptyText annotation is using to check that value has text before storing it 
	 *  
	 *  It does not check on selects and data retrieval operations
     *
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface NotEmptyText {

	}
	
	/**
	 *  Email annotation is using to check that value has a valid email before storing it 
	 *  
	 *  It does not check on selects and data retrieval operations
     *
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Email {

	}
	
	/**
	 *  Number annotation is using to check that all letters in value are digits before storing it 
	 *  
	 *  It does not check on selects and data retrieval operations
     *
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Number {

	}
	
	
	/**
	 *  Alphabet annotation is using to check that all letters in value are in specific alphabet before storing it 
	 *  
	 *  It does not check on selects and data retrieval operations
     *
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Alphabet {

		char[] value();
		
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
	 *  LowerCase annotation is using to ensure that value is in lower case before storing it
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
	 *  UpperCase annotation is using to ensure that value is in upper case before storing it
	 *
	 *  It does not have effect on selects and data retrieval operations
	 *
	 */	
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UpperCase {

	}
	
	/**
	 *  Pattern annotation is LowerCase annotation is using to ensure that value is upper case before storing it
	 *
	 *  It does not have effect on selects and data retrieval operations
	 *
	 */	
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Pattern {

		/**
		 * User defined regex expression to check match of the value
		 * 
		 * @return Regex pattern
		 */
		
		String value();
		
	}
	
	/**
	 *  Custom annotation is using special implementation to check value before storing it
	 *
	 *  It does not have effect on selects and data retrieval operations
	 *
	 */	
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Custom {

		/**
		 * Defines class name of the custom implementation of the checker.
		 * Class must implement special interface for this and be thread-safe and do not relay that it will be a singleton.
		 * 
		 * @return className of the custom implementation
		 */
		
		String value();
		
	}
	
}
