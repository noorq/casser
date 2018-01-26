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
import javax.validation.Constraint;
import net.helenus.mapping.validator.*;

/**
 * Constraint annotations are using for data integrity mostly for @java.lang.String types. The place
 * of the annotation is the particular method in model interface.
 *
 * <p>All of them does not have effect on selects and data retrieval operations.
 *
 * <p>Support types: - @NotNull supports any @java.lang.Object type - All annotations
 * support @java.lang.String type
 */
public final class Constraints {

  private Constraints() {}

  /**
   * NotNull annotation is using to check that value is not null before storing it
   *
   * <p>Applicable to use in any @java.lang.Object
   *
   * <p>It does not check on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = NotNullValidator.class)
  public @interface NotNull {}

  /**
   * NotEmpty annotation is using to check that value has text before storing it
   *
   * <p>Also checks for the null and it is more strict annotation then @NotNull
   *
   * <p>Can be used for @java.lang.CharSequence, @ByteBuffer and any array
   *
   * <p>It does not check on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = NotEmptyValidator.class)
  public @interface NotEmpty {}

  /**
   * Email annotation is using to check that value has a valid email before storing it
   *
   * <p>Can be used only for @CharSequence
   *
   * <p>It does not check on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = EmailValidator.class)
  public @interface Email {}

  /**
   * Number annotation is using to check that all letters in value are digits before storing it
   *
   * <p>Can be used only for @java.lang.CharSequence
   *
   * <p>It does not check on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = NumberValidator.class)
  public @interface Number {}

  /**
   * Alphabet annotation is using to check that all letters in value are in specific alphabet before
   * storing it
   *
   * <p>Can be used only for @java.lang.CharSequence
   *
   * <p>It does not check on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = AlphabetValidator.class)
  public @interface Alphabet {

    /**
     * Defines alphabet that will be used to check value
     *
     * @return alphabet characters in the string
     */
    String value();
  }

  /**
   * Length annotation is using to ensure that value has exact length before storing it
   *
   * <p>Can be used for @java.lang.CharSequence, @ByteBuffer and any array
   *
   * <p>It does not have effect on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = LengthValidator.class)
  public @interface Length {

    int value();
  }

  /**
   * MaxLength annotation is using to ensure that value has length less or equal to some threshold
   * before storing it
   *
   * <p>Can be used for @java.lang.CharSequence, @ByteBuffer and byte[]
   *
   * <p>It does not have effect on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = MaxLengthValidator.class)
  public @interface MaxLength {

    int value();
  }

  /**
   * MinLength annotation is using to ensure that value has length greater or equal to some
   * threshold before storing it
   *
   * <p>Can be used for @java.lang.CharSequence, @ByteBuffer and byte[]
   *
   * <p>It does not have effect on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = MinLengthValidator.class)
  public @interface MinLength {

    int value();
  }

  /**
   * LowerCase annotation is using to ensure that value is in lower case before storing it
   *
   * <p>Can be used only for @java.lang.CharSequence
   *
   * <p>It does not have effect on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = LowerCaseValidator.class)
  public @interface LowerCase {}

  /**
   * UpperCase annotation is using to ensure that value is in upper case before storing it
   *
   * <p>Can be used only for @java.lang.CharSequence
   *
   * <p>It does not have effect on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = UpperCaseValidator.class)
  public @interface UpperCase {}

  /**
   * Pattern annotation is LowerCase annotation is using to ensure that value is upper case before
   * storing it
   *
   * <p>Can be used only for @java.lang.CharSequence
   *
   * <p>It does not have effect on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = PatternValidator.class)
  public @interface Pattern {

    /**
     * User defined regex expression to check match of the value
     *
     * @return Java regex pattern
     */
    String value();

    /**
     * Regex flags composition
     *
     * @return Java regex flags
     */
    int flags();
  }

  /**
   * Distinct annotation is used to signal, but not ensure that a value should be distinct in the
   * database.
   *
   * <p>Can be used only for @java.lang.CharSequence
   *
   * <p>It does not have effect on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = DistinctValidator.class)
  public @interface Distinct {

    /**
     * User defined list of properties that combine with this one to result in a distinct
     * combination in the table.
     *
     * @return Java
     */
    String[] value() default "";

    boolean alone() default true;

    boolean combined() default true;
  }

  /**
   * Distinct annotation is used to signal, but not ensure that a value should be distinct in the
   * database.
   *
   * <p>Can be used only for @java.lang.CharSequence
   *
   * <p>It does not have effect on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = OneToOneRelationshipValidator.class)
  public @interface OneToOne {

    /**
     * User defined list of properties that combine with this one to result in a distinct
     * combination in the table.
     *
     * @return Java
     */
    String[] value() default "";
  }

  /**
   * Distinct annotation is used to signal, but not ensure that a value should be distinct in the
   * database.
   *
   * <p>Can be used only for @java.lang.CharSequence
   *
   * <p>It does not have effect on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = OneToManyRelationshipValidator.class)
  public @interface OneToMany {

    /**
     * User defined list of properties that combine with this one to result in a distinct
     * combination in the table.
     *
     * @return Java
     */
    String[] value() default "";
  }

  /**
   * Distinct annotation is used to signal, but not ensure that a value should be distinct in the
   * database.
   *
   * <p>Can be used only for @java.lang.CharSequence
   *
   * <p>It does not have effect on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = ManyToOneRelationshipValidator.class)
  public @interface ManyToOne {

    /**
     * User defined list of properties that combine with this one to result in a distinct
     * combination in the table.
     *
     * @return Java
     */
    String[] value() default "";
  }

  /**
   * Distinct annotation is used to signal, but not ensure that a value should be distinct in the
   * database.
   *
   * <p>Can be used only for @java.lang.CharSequence
   *
   * <p>It does not have effect on selects and data retrieval operations
   */
  @Documented
  @Retention(RetentionPolicy.RUNTIME)
  @Target(value = {ElementType.METHOD, ElementType.ANNOTATION_TYPE})
  @Constraint(validatedBy = ManyToManyRelationshipValidator.class)
  public @interface ManyToMany {

    /**
     * User defined list of properties that combine with this one to result in a distinct
     * combination in the table.
     *
     * @return Java
     */
    String[] value() default "";
  }
}
