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
 * Column annotation is used to define additional properties of the column in entity mapping
 * interfaces: @Table, @UDT, @Tuple
 *
 * <p>Column annotation can be used to override default name of the column or to setup order of the
 * columns in the mapping
 *
 * <p>Usually for @Table and @UDT types it is not important to define order of the columns, but
 * in @Tuple mapping it is required, because tuple itself represents the sequence of the types with
 * particular order in the table's column
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
public @interface Column {

  /**
   * Default value is the name of the method normalized to underscore
   *
   * @return name of the column
   */
  String value() default "";

  /**
   * Ordinal will be used for ascending sorting of columns
   *
   * <p>Default value is 0, because not all mapping entities require all fields to have unique
   * ordinals, only @Tuple mapping entity requires all of them to be unique.
   *
   * @return number that used to sort columns, usually for @Tuple only
   */
  int ordinal() default 0;

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
   * Used to determine if mutations (insert, upsert, update) can be retried by the server. When all
   * fields in a query are idempotent the query is marked idempotent. Optionally, a user can
   * explicitly mark a query idempotent even if all fields are not marked as such.
   *
   * @return
   */
  boolean idempotent() default false;
}
