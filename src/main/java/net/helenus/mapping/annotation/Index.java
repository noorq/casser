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

import java.lang.annotation.*;

/**
 * Index annotation is using under the specific column or method in entity interface with @Table
 * annotation.
 *
 * <p>The corresponding secondary index will be created in the underline @Table for the specific
 * column.
 *
 * <p>Currently Cassandra supports only single column index, so this index works only for single
 * column.
 *
 * <p>Make sure that you are using low cardinality columns for this index, that is the requirement
 * of the Cassandra. Low cardinality fields examples: gender, country, age, status and etc High
 * cardinality fields examples: id, email, timestamp, UUID and etc
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface Index {

  /**
   * Defined the name of the index. By default will be used the column name.
   *
   * @return name of the index
   */
  String value() default "";

  /**
   * For reserved words in Cassandra we need quotation in CQL queries. This property marks that the
   * name of the UDT type needs to be quoted.
   *
   * <p>Default value is false, we are quoting only selected names.
   *
   * @return true if name have to be quoted
   */
  boolean forceQuote() default false;

  /**
   * Create a case-insensitive index using Cassandra 3.x+ support for SASI indexing.
   *
   * @return true if the index should ignore case when comparing
   */
  boolean caseSensitive() default true;

  /** @return */
  boolean distinct() default false;
}
