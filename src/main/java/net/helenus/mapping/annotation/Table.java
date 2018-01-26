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
 * Entity annotation
 *
 * <p>Table annotation is used to define Table mapping to some interface
 *
 * <p>There are three types of Entity mapping annotations: @Table, @UDT, @Tuple
 *
 * <p>For each @Table annotated interface Helenus will create/update/verify Cassandra Table and some
 * indexes if needed on startup.
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Table {

  /**
   * Default value is the SimpleName of the interface normalized to underscore
   *
   * @return name of the UDT type
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
}
