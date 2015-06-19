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

/**
 *  Entity annotation
 *  
 *  UDT annotation is used to define the UDT (User Defined Type) mapping for some interface
 *  
 *  There are three types of Entity mapping annotations: @Table, @UDT, @Tuple
 *  
 *  For each annotated @UDT type Casser will create/update/verify Cassandra Type on startup
 *  
 *  @author Albert Shift
 */

import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ ElementType.TYPE })
public @interface UDT {

	/**
	 * Default value is the SimpleName of the interface normalized to underscore 
	 * 
	 * @return name of the UDT type
	 */
	
	String value() default "";

	/**
	 * For reserved words in Cassandra we need quotation in CQL queries. This property marks that
	 * the name of the UDT type needs to be quoted.
	 * 
	 * Default value is false, we are quoting only selected names.
	 * 
	 * @return true if name have to be quoted
	 */
	
	boolean forceQuote() default false;
	
}
