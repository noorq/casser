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
package net.helenus.mapping.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * PartitionKey annotation is using to define that particular column is the part
 * of partition key in the table.
 *
 * <p>
 * Partition Key is the routing key. Cassandra is using it to find the primary
 * data node in the cluster that holds data. Cassandra combines all parts of the
 * partition key to byte array and then calculates hash function by using good
 * distribution algorithm (by default MurMur3). After that it uses hash number
 * as a token in the ring to find a virtual and then a physical data server.
 *
 * <p>
 * For @Table mapping entity it is required to have as minimum one PartitionKey
 * column. For @UDT and @Tuple mapping entities @PartitionKey annotation is not
 * using.
 */
@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface PartitionKey {

	/**
	 * Default value is the name of the method normalized to underscore
	 *
	 * @return name of the column
	 */
	String value() default "";

	/**
	 * PartitionKey parts must be ordered in the @Table. It is the requirement of
	 * Cassandra. That is how the partition key calculation works, column parts will
	 * be joined based on some order and final hash/token will be calculated.
	 *
	 * <p>
	 * Be default ordinal has 0 value, that's because in most cases @Table have
	 * single column for @PartitionKey If you have 2 and more parts of the
	 * PartitionKey, then you need to use ordinal() to define the sequence of the
	 * parts
	 *
	 * @return number that used to sort columns in PartitionKey
	 */
	int ordinal() default 0;

	/**
	 * For reserved words in Cassandra we need quotation in CQL queries. This
	 * property marks that the name of the UDT type needs to be quoted.
	 *
	 * <p>
	 * Default value is false, we are quoting only selected names.
	 *
	 * @return true if name have to be quoted
	 */
	boolean forceQuote() default false;
}
