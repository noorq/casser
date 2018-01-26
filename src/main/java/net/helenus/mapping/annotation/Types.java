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

import com.datastax.driver.core.DataType;
import java.lang.annotation.*;

/**
 * Types annotations are using for clarification of Cassandra data type for particular Java type.
 *
 * <p>Sometimes it is possible to have for single Java type multiple Cassandra data types: - @String
 * can be @DataType.Name.ASCII or @DataType.Name.TEXT or @DataType.Name.VARCHAR - @Long can
 * be @DataType.Name.BIGINT or @DataType.Name.COUNTER
 *
 * <p>All those type annotations simplify mapping between Java types and Cassandra data types. They
 * are not required, for each Java type there is a default Cassandra data type in Helenus, but in
 * some cases you would like to control mapping to make sure that the right Cassandra data type is
 * using.
 *
 * <p>For complex types like collections, UDF and Tuple types all those annotations are using to
 * clarify the sub-type(s) or class/UDF names.
 *
 * <p>Has significant effect on schema operations.
 */
public final class Types {

  private Types() {}

  /** Says to use @DataType.Name.ASCII data type in schema Java type is @String */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface Ascii {}

  /** Says to use @DataType.Name.BIGINT data type in schema Java type is @Long */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface Bigint {}

  /**
   * Says to use @DataType.Name.BLOB data type in schema Java type is @ByteBuffer or @byte[] Using
   * by default
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface Blob {}

  /**
   * Says to use @DataType.Name.LIST data type in schema with specific sub-type Java type is @List
   *
   * <p>Helenus does not allow to use a specific implementation of the collection thereof data
   * retrieval operation result can be a collection with another implementation.
   *
   * <p>This annotation is usually used only for sub-types clarification and only in case if
   * sub-type is Java type that corresponds to multiple Cassandra data types.
   *
   * <p>For this type there are special operations: prepend, prependAll, setIdx, append, appendAll,
   * discard and discardAll in @UpdateOperation
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface List {

    /**
     * Clarification of using the sub-type data type in the collection. It supports only simple data
     * type (not Collection, UDT or Tuple)
     *
     * <p>In case if you need UDT sub-type in the list, consider @UDTList annotation
     *
     * @return data type name of the value
     */
    DataType.Name value();
  }

  /**
   * Says to use @DataType.Name.MAP data type in schema with specific sub-types Java type is @Map
   *
   * <p>Helenus does not allow to use a specific implementation of the collection thereof data
   * retrieval operation result can be a collection with another implementation.
   *
   * <p>This annotation is usually used only for sub-types clarification and only in case if
   * sub-type is Java type that corresponds to multiple Cassandra data types.
   *
   * <p>For this type there are special operations: put and putAll in @UpdateOperation.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface Map {

    /**
     * Clarification of using the sub-type data type in the collection. It supports only simple data
     * type (not Collection, UDT or Tuple)
     *
     * <p>In case if you need UDT key sub-type in the map, consider @UDTKeyMap or @UDTMap
     * annotations
     *
     * @return data type name of the key
     */
    DataType.Name key();

    /**
     * Clarification of using the sub-type data type in the collection. It supports only simple data
     * type (not Collection, UDT or Tuple)
     *
     * <p>In case if you need UDT value sub-type in the map, consider @UDTValueMap or @UDTMap
     * annotations
     *
     * @return data type name of the value
     */
    DataType.Name value();
  }

  /**
   * Says to use @DataType.Name.COUNTER type in schema Java type is @Long
   *
   * <p>For this type there are special operations: increment and decrement in @UpdateOperation. You
   * do not need to initialize counter value, it will be done automatically by Cassandra.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface Counter {}

  /**
   * Says to use @DataType.Name.SET data type in schema with specific sub-type Java type is @Set
   *
   * <p>Helenus does not allow to use a specific implementation of the collection thereof data
   * retrieval operation result can be a collection with another implementation.
   *
   * <p>This annotation is usually used only for sub-types clarification and only in case if
   * sub-type is Java type that corresponds to multiple Cassandra data types.
   *
   * <p>For this type there are special operations: add, addAll, remove and removeAll
   * in @UpdateOperation.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface Set {

    /**
     * Clarification of using the sub-type data type in the collection. It supports only simple data
     * type (not Collection, UDT or Tuple)
     *
     * <p>In case if you need UDT sub-type in the set, consider @UDTSet annotation
     *
     * @return data type name of the value
     */
    DataType.Name value();
  }

  /**
   * Says to use @DataType.Name.CUSTOM type in schema Java type is @ByteBuffer or @byte[]
   *
   * <p>Uses for custom user types that has special implementation. Helenus does not deal with this
   * class directly for now, uses only in serialized form.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface Custom {

    /**
     * Class name of the custom user type that is implementation of the type
     *
     * @return class name of the custom type implementation
     */
    String className();
  }

  /** Says to use @DataType.Name.TEXT type in schema Java type is @String Using by default */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface Text {}

  /** Says to use @DataType.Name.TIMESTAMP type in schema Java type is @Date Using by default */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface Timestamp {}

  /** Says to use @DataType.Name.TIMEUUID type in schema Java type is @UUID or @Date */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface Timeuuid {}

  /**
   * Says to use @DataType.Name.TUPLE type in schema Java type is @TupleValue or model interface
   * with @Tuple annotation
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  public @interface Tuple {

    /**
     * If Java type is the @TupleValue then this field is required. Any Cassandra Tuple is the
     * sequence of Cassandra types. For now Helenus supports only simple data types in tuples
     * for @TupleValue Java type
     *
     * <p>In case if Java type is the model interface with @Tuple annotation then all methods in
     * this interface can have Types annotations that can be complex types as well.
     *
     * @return data type name sequence
     */
    DataType.Name[] value() default {};
  }

  /**
   * Says to use @DataType.Name.UDT type in schema Java type is @UDTValue or model interface
   * with @UDT annotation
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface UDT {

    /**
     * If Java type is the @UDTValue then this field is required. Any Cassandra UDT has name and
     * must be created before this use as a Cassandra Type.
     *
     * <p>This value is the UDT name of the Cassandra Type that was already created in the schema
     *
     * <p>In case of Java type is the model interface with @UDT annotation then this field is not
     * using since model interface defines UserDefinedType with specific name
     *
     * @return UDT name
     */
    String value() default "";

    /**
     * Only used for JavaType @UDTValue
     *
     * <p>In case if value() method returns reserved word that can not be used as a name of UDT then
     * forceQuote will add additional quotes around this name in all CQL queries.
     *
     * <p>Default value is false.
     *
     * @return true if quotation is needed
     */
    boolean forceQuote() default false;
  }

  /**
   * Says to use @DataType.Name.MAP data type in schema with specific UDT sub-type as a key and
   * simple sub-type as a value Java type is @Map
   *
   * <p>Helenus does not allow to use a specific implementation of the collection thereof data
   * retrieval operation result can be a collection with another implementation.
   *
   * <p>This annotation is usually used only for sub-types clarification and only in case if
   * sub-type is Java type that corresponds to multiple Cassandra data types.
   *
   * <p>For this type there are special operations: put and putAll in @UpdateOperation.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface UDTKeyMap {

    /**
     * Clarification of using the UDT data type as a key sub-type in the collection.
     *
     * @return annotation of UDT type
     */
    UDT key();

    /**
     * Clarification of using the sub-type data type in the collection. It supports only simple data
     * type (not Collection, UDT or Tuple)
     *
     * <p>In case if you need UDT value sub-type in the map, consider @UDTMap annotations
     *
     * @return data type name of the value
     */
    DataType.Name value();
  }

  /**
   * Says to use @DataType.Name.LIST data type in schema with specific UDT sub-type Java type
   * is @List
   *
   * <p>Helenus does not allow to use a specific implementation of the collection thereof data
   * retrieval operation result can be a collection with another implementation.
   *
   * <p>This annotation is usually used only for sub-types clarification and only in case if
   * sub-type is Java type that corresponds to multiple Cassandra data types.
   *
   * <p>For this type there are special operations: prepend, prependAll, setIdx, append, appendAll,
   * discard and discardAll in @UpdateOperation
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface UDTList {

    /**
     * Clarification of using the UDT data type as a sub-type in the collection.
     *
     * @return annotation of the UDT value
     */
    UDT value();
  }

  /**
   * Says to use @DataType.Name.MAP data type in schema with specific UDT sub-types Java type
   * is @Map
   *
   * <p>Helenus does not allow to use a specific implementation of the collection thereof data
   * retrieval operation result can be a collection with another implementation.
   *
   * <p>This annotation is usually used only for sub-types clarification and only in case if
   * sub-type is Java type that corresponds to multiple Cassandra data types.
   *
   * <p>For this type there are special operations: put and putAll in @UpdateOperation.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface UDTMap {

    /**
     * Clarification of using the UDT data type as a key sub-type in the collection.
     *
     * @return annotation of the UDT key
     */
    UDT key();

    /**
     * Clarification of using the UDT data type as a value sub-type in the collection.
     *
     * @return annotation of the UDT value
     */
    UDT value();
  }

  /**
   * Says to use @DataType.Name.SET data type in schema with specific UDT sub-type Java type is @Set
   *
   * <p>Helenus does not allow to use a specific implementation of the collection thereof data
   * retrieval operation result can be a collection with another implementation.
   *
   * <p>This annotation is usually used only for sub-types clarification and only in case if
   * sub-type is Java type that corresponds to multiple Cassandra data types.
   *
   * <p>For this type there are special operations: add, addAll, remove and removeAll
   * in @UpdateOperation.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface UDTSet {

    /**
     * Clarification of using the UDT data type as a sub-type in the collection.
     *
     * @return annotation of the UDT value
     */
    UDT value();
  }

  /**
   * Says to use @DataType.Name.MAP data type in schema with specific simple sub-type as a key and
   * UDT sub-type as a value Java type is @Map
   *
   * <p>Helenus does not allow to use a specific implementation of the collection thereof data
   * retrieval operation result can be a collection with another implementation.
   *
   * <p>This annotation is usually used only for sub-types clarification and only in case if
   * sub-type is Java type that corresponds to multiple Cassandra data types.
   *
   * <p>For this type there are special operations: put and putAll in @UpdateOperation.
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface UDTValueMap {

    /**
     * Clarification of using the sub-type data type in the collection. It supports only simple data
     * type (not Collection, UDT or Tuple)
     *
     * <p>In case if you need UDT key sub-type in the map, consider @UDTMap annotations
     *
     * @return data type name of the key
     */
    DataType.Name key();

    /**
     * Clarification of using the UDT data type as a value sub-type in the collection.
     *
     * @return annotation of the UDT value
     */
    UDT value();
  }

  /** Says to use @DataType.Name.UUID type in schema Java type is @UUID Using by default */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface Uuid {}

  /** Says to use @DataType.Name.VARCHAR type in schema Java type is @String */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  public @interface Varchar {}
}
