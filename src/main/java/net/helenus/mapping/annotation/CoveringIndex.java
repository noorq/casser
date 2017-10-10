package net.helenus.mapping.annotation;

import net.helenus.core.Getter;

import java.lang.annotation.*;

/**
 * CoveringIndex annotation is using under the specific column or method in entity interface
 * with @Table annotation.
 *
 * <p>A corresponding materialized view will be created based on the underline @Table for the
 * specific column.
 *
 * <p>This is useful when you need to perform IN or SORT/ORDER-BY queries and to do so you'll need
 * different materialized table on disk in Cassandra.
 *
 * <p>For each @Table annotated interface Helenus will create/update/verify Cassandra Materialized Views
 * and some indexes if needed on startup.
 */
@Inherited
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface CoveringIndex {

    /**
     * Defined the name of the index. By default the entity name with column name as suffix.
     *
     * @return name of the covering index
     */
    String name() default "";

    /**
     * Set of fields in this entity to replicate in the index.
     *
     * @return array of the string names of the fields.
     */
    String[] covering() default "";

    /**
     * Set of fields to use as the partition keys for this projection.
     *
     * @return array of the string names of the fields.
     */
    String[] partitionKeys() default "";

    /**
     * Set of fields to use as the clustering columns for this projection.
     *
     * @return array of the string names of the fields.
     */
    String[] clusteringColumns() default "";

}
