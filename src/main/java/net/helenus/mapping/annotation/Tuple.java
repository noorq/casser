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
 * <p>Tuple annotation is used to define Tuple type mapping to some interface
 *
 * <p>There are three types of Entity mapping annotations: @Table, @UDT, @Tuple
 *
 * <p>Tuple is fully embedded type, it is the sequence of the underline types and the order of the
 * sub-types is important, therefore all @Column-s must have ordinal() and only @Column annotation
 * supported for underline types
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Tuple {}
