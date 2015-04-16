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

import com.datastax.driver.core.DataType;

public final class Types {

	private Types() {
	}
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Ascii {

	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Bigint {

	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Blob {

	}
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface List {
		
		DataType.Name value();
		
	}
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Map {

		DataType.Name key();

		DataType.Name value();
		
	}
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Counter {

	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Set {
		
		DataType.Name value();
		
	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Custom {

		String className() default "";
		
	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Text {

	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Timestamp {

	}
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Timeuuid {

	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	public @interface Tuple {
	
		DataType.Name[] value() default {};
	
	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UDT {

		String value() default "";

		boolean forceQuote() default false;
		
	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UDTKeyMap {
		
		UDT key();
		
		DataType.Name value();
		
	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UDTList {
		
		UDT value();
		
	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UDTMap {
		
		UDT key();
		
		UDT value();
		
	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UDTSet {
		
		UDT value();
		
	}
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UDTValueMap {
		
		DataType.Name key();
		
		UDT value();
		
	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Uuid {

	}

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Varchar {

	}

}
