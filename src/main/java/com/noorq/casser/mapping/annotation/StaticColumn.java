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

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @StaticColumn annotation is using to define a static column in Cassandra Table
 * 
 * It does not have effect in @UDT and @Tuple types and in @Table-s that does not have @ClusteringColumn-s 
 * 
 * In case of using @ClusteringColumn we can repeat some information that is unique for a row.
 * For this purpose we can define @StaticColumn annotation, that will create static column in the table
 * 
 * @author Albert Shift
 *
 */

@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
public @interface StaticColumn {

	/**
	 * Default value is the name of the method normalized to underscore 
	 * 
	 * @return name of the column
	 */
	
	String value() default "";
	
	/**
	 * Ordinal will be used for ascending sorting of static columns
	 * 
	 * @return number that used to sort columns in PartitionKey
	 */
	
	int ordinal() default 0;

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
