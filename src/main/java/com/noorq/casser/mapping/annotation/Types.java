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

/**
 * Types annotations are using for clarification of Cassandra data type for particular Java type.
 * 
 * Sometimes it is possible to have for single Java type multiple Cassandra data types:
 * - @String can be @DataType.Name.ASCII or @DataType.Name.TEXT or @DataType.Name.VARCHAR
 * - @Long can be @DataType.Name.BIGINT or @DataType.Name.COUNTER
 * 
 * All those type annotations simplify mapping between Java types and Cassandra data types.
 * They are not required, for each Java type there is a default Cassandra data type in Casser, but in some
 * cases you would like to control mapping to make sure that the right Cassandra data type is using. 
 * 
 * For complex types like collections, UDF and Tuple types all those annotations are using to
 * clarify the sub-type(s) or class/UDF names.
 * 
 * Has significant effect on schema operations.
 * 
 * @author Albert Shift
 *
 */

public final class Types {

	private Types() {
	}
	
	/**
	 * Says to use @DataType.Name.ASCII data type in schema
	 * Java type is @String 
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Ascii {

	}

	/**
	 * Says to use @DataType.Name.BIGINT data type in schema
	 * Java type is @Long 
	 */

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Bigint {

	}

	/**
	 * Says to use @DataType.Name.BLOB data type in schema
	 * Java type is @ByteBuffer or @byte[]
	 * Using by default
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Blob {

	}
	
	/**
	 * Says to use @DataType.Name.LIST data type in schema with specific sub-type
     * Java type is @List
	 *
	 * Casser does not allow to use a specific implementation of the collection thereof data retrieval operation
	 * result can be a collection with another implementation.
	 * 
	 * This annotation is usually used only for sub-types clarification and only in case if sub-type is Java type that
	 * corresponds to multiple Cassandra data types.
	 * 
	 * For this type there are special operations: prepend, prependAll, setIdx, append, appendAll, discard and discardAll in @UpdateOperation
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface List {
		
		/**
		 * Clarification of using the sub-type data type in the collection.
		 * It supports only simple data type (not Collection, UDT or Tuple)
		 * 
		 * In case if you need UDT sub-type in the list, consider @UDTList annotation
		 */
		
		DataType.Name value();
		
	}
	
	/**
	 * Says to use @DataType.Name.MAP data type in schema with specific sub-types
     * Java type is @Map
	 *
	 * Casser does not allow to use a specific implementation of the collection thereof data retrieval operation
	 * result can be a collection with another implementation.
	 * 
	 * This annotation is usually used only for sub-types clarification and only in case if sub-type is Java type that
	 * corresponds to multiple Cassandra data types.
	 * 
	 * For this type there are special operations: put and putAll in @UpdateOperation.
	 * 
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Map {

		/**
		 * Clarification of using the sub-type data type in the collection.
		 * It supports only simple data type (not Collection, UDT or Tuple)
		 * 
		 * In case if you need UDT key sub-type in the map, consider @UDTKeyMap or @UDTMap annotations
		 */
		
		DataType.Name key();
		
		/**
		 * Clarification of using the sub-type data type in the collection.
		 * It supports only simple data type (not Collection, UDT or Tuple)
		 * 
		 * In case if you need UDT value sub-type in the map, consider @UDTValueMap or @UDTMap annotations
		 */
		
		DataType.Name value();
		
	}
	
	/**
	 * Says to use @DataType.Name.COUNTER type in schema
	 * Java type is @Long 
	 * 
	 * For this type there are special operations: increment and decrement in @UpdateOperation.
	 * You do not need to initialize counter value, it will be done automatically by Cassandra.
	 */	
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Counter {

	}

	/**
	 * Says to use @DataType.Name.SET data type in schema with specific sub-type
     * Java type is @Set
	 *
	 * Casser does not allow to use a specific implementation of the collection thereof data retrieval operation
	 * result can be a collection with another implementation.
	 * 
	 * This annotation is usually used only for sub-types clarification and only in case if sub-type is Java type that
	 * corresponds to multiple Cassandra data types.
	 * 
	 * For this type there are special operations: add, addAll, remove and removeAll in @UpdateOperation.
	 * 
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Set {
		
		/**
		 * Clarification of using the sub-type data type in the collection.
		 * It supports only simple data type (not Collection, UDT or Tuple)
		 * 
		 * In case if you need UDT sub-type in the set, consider @UDTSet annotation
		 */
		
		DataType.Name value();
		
	}

	/**
	 * Says to use @DataType.Name.CUSTOM type in schema
	 * Java type is @ByteBuffer or @byte[]
	 * 
	 * Uses for custom user types that has special implementation.
	 * Casser does not deal with this class directly for now, uses only in serialized form. 
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Custom {

		/**
		 *  Class name of the custom user type that is implementation of the type
		 */
		
		String className();
		
	}

	/**
	 * Says to use @DataType.Name.TEXT type in schema
	 * Java type is @String 
	 * Using by default
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Text {

	}

	/**
	 * Says to use @DataType.Name.TIMESTAMP type in schema
	 * Java type is @Date 
	 * Using by default
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Timestamp {

	}
	
	/**
	 * Says to use @DataType.Name.TIMEUUID type in schema
	 * Java type is @UUID or @Date 
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Timeuuid {

	}

	/**
	 * Says to use @DataType.Name.TUPLE type in schema
	 * Java type is @TupleValue or model interface with @Tuple annotation
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	public @interface Tuple {

		/**
		 *  If Java type is the @TupleValue then this field is required.
		 *  Any Cassandra Tuple is the sequence of Cassandra types. 
		 *  For now Casser supports only simple data types in tuples for @TupleValue Java type
		 *  
		 *  In case if Java type is the model interface with @Tuple annotation then
		 *  all methods in this interface can have Types annotations that can be complex types as well.
		 */
		
		DataType.Name[] value() default {};
	
	}

	/**
	 * Says to use @DataType.Name.UDT type in schema
	 * Java type is @UDTValue or model interface with @UDT annotation
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UDT {

		/**
		 *  If Java type is the @UDTValue then this field is required.
		 *  Any Cassandra UDT has name and must be created before this use as a Cassandra Type.
		 *  
		 *  This value is the UDT name of the Cassandra Type that was already created in the schema
		 *  
		 *  In case of Java type is the model interface with @UDT annotation then
		 *  this field is not using since model interface defines UserDefinedType with specific name
		 */
		
		String value() default "";

		/**
		 *  Only used for JavaType @UDTValue 
		 *  
		 *  In case if value() method returns reserved word that can not be used as a name of UDT then
		 *  forceQuote will add additional quotes around this name in all CQL queries.
		 *  
		 *  Default value is false.
		 */
		
		boolean forceQuote() default false;
		
	}

	/**
	 * Says to use @DataType.Name.MAP data type in schema with specific UDT sub-type as a key and simple sub-type as a value
     * Java type is @Map
	 *
	 * Casser does not allow to use a specific implementation of the collection thereof data retrieval operation
	 * result can be a collection with another implementation.
	 * 
	 * This annotation is usually used only for sub-types clarification and only in case if sub-type is Java type that
	 * corresponds to multiple Cassandra data types.
	 * 
	 * For this type there are special operations: put and putAll in @UpdateOperation.
	 * 
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UDTKeyMap {
		
		/**
		 * Clarification of using the UDT data type as a key sub-type in the collection.
		 */
		
		UDT key();
		
		/**
		 * Clarification of using the sub-type data type in the collection.
		 * It supports only simple data type (not Collection, UDT or Tuple)
		 * 
		 * In case if you need UDT value sub-type in the map, consider @UDTMap annotations
		 */		
		
		DataType.Name value();
		
	}

	/**
	 * Says to use @DataType.Name.LIST data type in schema with specific UDT sub-type
     * Java type is @List
	 *
	 * Casser does not allow to use a specific implementation of the collection thereof data retrieval operation
	 * result can be a collection with another implementation.
	 * 
	 * This annotation is usually used only for sub-types clarification and only in case if sub-type is Java type that
	 * corresponds to multiple Cassandra data types.
	 * 
	 * For this type there are special operations: prepend, prependAll, setIdx, append, appendAll, discard and discardAll in @UpdateOperation
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UDTList {
		
		/**
		 * Clarification of using the UDT data type as a sub-type in the collection.
		 */
		
		UDT value();
		
	}

	/**
	 * Says to use @DataType.Name.MAP data type in schema with specific UDT sub-types
     * Java type is @Map
	 *
	 * Casser does not allow to use a specific implementation of the collection thereof data retrieval operation
	 * result can be a collection with another implementation.
	 * 
	 * This annotation is usually used only for sub-types clarification and only in case if sub-type is Java type that
	 * corresponds to multiple Cassandra data types.
	 * 
	 * For this type there are special operations: put and putAll in @UpdateOperation.
	 * 
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UDTMap {
		
		/**
		 * Clarification of using the UDT data type as a key sub-type in the collection.
		 */		
		
		UDT key();
		
		/**
		 * Clarification of using the UDT data type as a value sub-type in the collection.
		 */
		
		UDT value();
		
	}

	/**
	 * Says to use @DataType.Name.SET data type in schema with specific UDT sub-type
     * Java type is @Set
	 *
	 * Casser does not allow to use a specific implementation of the collection thereof data retrieval operation
	 * result can be a collection with another implementation.
	 * 
	 * This annotation is usually used only for sub-types clarification and only in case if sub-type is Java type that
	 * corresponds to multiple Cassandra data types.
	 * 
	 * For this type there are special operations: add, addAll, remove and removeAll in @UpdateOperation.
	 * 
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UDTSet {
		
		/**
		 * Clarification of using the UDT data type as a sub-type in the collection.
		 */		
		
		UDT value();
		
	}
	
	/**
	 * Says to use @DataType.Name.MAP data type in schema with specific simple sub-type as a key and UDT sub-type as a value
     * Java type is @Map
	 *
	 * Casser does not allow to use a specific implementation of the collection thereof data retrieval operation
	 * result can be a collection with another implementation.
	 * 
	 * This annotation is usually used only for sub-types clarification and only in case if sub-type is Java type that
	 * corresponds to multiple Cassandra data types.
	 * 
	 * For this type there are special operations: put and putAll in @UpdateOperation.
	 * 
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface UDTValueMap {
		
		/**
		 * Clarification of using the sub-type data type in the collection.
		 * It supports only simple data type (not Collection, UDT or Tuple)
		 * 
		 * In case if you need UDT key sub-type in the map, consider @UDTMap annotations
		 */		
		
		DataType.Name key();
		
		/**
		 * Clarification of using the UDT data type as a value sub-type in the collection.
		 */		
		
		UDT value();
		
	}
	
	/**
	 * Says to use @DataType.Name.UUID type in schema
	 * Java type is @UUID 
	 * Using by default
	 */

	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Uuid {

	}

	/**
	 * Says to use @DataType.Name.VARCHAR type in schema
	 * Java type is @String 
	 */
	
	@Documented
	@Retention(RetentionPolicy.RUNTIME)
	@Target(value = { ElementType.METHOD, ElementType.ANNOTATION_TYPE })
	public @interface Varchar {

	}

}
