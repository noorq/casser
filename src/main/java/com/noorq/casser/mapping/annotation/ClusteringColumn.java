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

import com.noorq.casser.mapping.OrderingDirection;

/**
 * @ClusteringColumn is the family column in legacy Cassandra API
 * 
 * The purpose of this column is have additional dimension in the table.
 * Both @PartitionKey and @ClusteringColumn together are parts of the primary key of the table.
 * The primary difference between them is that the first one is using for routing purposes 
 * in order to locate a data node in the cluster, otherwise the second one is using
 * inside the node to locate peace of data in concrete machine. 
 * 
 * ClusteringColumn can be represented as a Key in SortedMap that fully stored in a single node.
 * All developers must be careful for selecting fields for clustering columns, because all data
 * inside this SortedMap must fit in to one node.
 * 
 * ClusteringColumn can have more than one part and the order of parts is important.
 * This order defines the way how Cassandra joins the parts and influence of data retrieval
 * operations. Each part can have ordering property that defines default ascending or descending order
 * of data. In case of two and more parts in select queries developer needs to have consisdent 
 * order of all parts as they defined in table. 
 * 
 * For example, first part is ASC ordering, second is also ASC, so Cassandra will sort entries like this:
 * a-a
 * a-b
 * b-a
 * b-b
 * In this case we are able run queries:
 *   ORDER BY first ASC, second ASC 
 *   ORDER BY first DESC, second DESC 
 *   WHERE first=? ORDER BY second ASC 
 *   WHERE first=? ORDER BY second DESC
 *   WHERE first=? AND second=?
 * 
 * But, we can not run queries:
 *   ORDER BY first DESC, second ASC 
 *   ORDER BY first ASC, second DESC 
 *   WHERE second=? ORDER BY first (ASC,DESC)
 * 
 * @author Albert Shift
 *
 */

@Retention(value = RetentionPolicy.RUNTIME)
@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
public @interface ClusteringColumn {

	/**
	 * Default value is the name of the method normalized to underscore 
	 * 
	 * @return name of the column
	 */
	
	String value() default "";
	
	/**
	 * ClusteringColumn parts must be ordered in the @Table. It is the requirement of Cassandra.
	 * Cassandra joins all parts to the final clustering key that is stored in column family name.
	 * Additionally all parts can have some ordering (ASC, DESC) that with sequence of parts
	 * determines key comparison function, so Cassandra storing column family names always in sorted order.
	 * 
     * Be default ordinal has 0 value, that's because in most cases @Table have single column for ClusteringColumn
	 * If you have 2 and more parts of the ClusteringColumn, then you need to use ordinal() to
	 * define the sequence of the parts	  
	 * 
	 * @return number that used to sort clustering columns
	 */
	
	int ordinal() default 0;

	/**
	 * Default order of values in the ClusteringColumn
	 * This ordering is using for comparison of the clustering column values when Cassandra stores it in the 
	 * sorted order.
	 * 
	 * Default value is the ascending order
	 * 
	 * @return ascending order or descending order of clustering column values
	 */
	
	OrderingDirection ordering() default OrderingDirection.ASC;
	
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
