package com.example.ontologyprocessor.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * Writing: a {@code Dataset} and a {@link TableRef} in, nothing out.
 *
 * <p>The same shape as {@code OntologyLoad}'s {@code Loader}, with the sink
 * named rather than baked in - and returning {@code void} for the same reason.
 * A write is the pipeline's action: it is what actually runs the plan the
 * lazy stages built, so there is nothing to hand back.
 *
 * <pre>{@code
 * Loader people = dataset -> store.write(dataset, TableRef.parse("ontology.people"), WriteMode.OVERWRITE);
 * }</pre>
 *
 * <p>A write does not create the table. A backend may create one implicitly if
 * that is natural for it - Elasticsearch will index into an unknown index and
 * infer a mapping - but a caller that wants a table of a known shape should
 * declare it through {@link DdlOperations#createTable(TableSpec)} first. Two
 * concerns, two contracts.
 *
 * <p>{@link Serializable}, for the same reason as {@link DataReader}.
 */
public interface DataWriter extends Serializable {

    /**
     * Writes the dataset to {@code table}.
     *
     * <p>Triggers a Spark job.
     *
     * @param dataset what to write, never {@code null}
     * @param table   where to write it, never {@code null}
     * @param mode    what to do with data already there
     * @param options backend-specific settings; {@link Options#none()} for none
     * @throws IoException if the write fails, or if {@code mode} is
     *         {@link WriteMode#ERROR_IF_EXISTS} and the target already holds data
     */
    void write(Dataset<Row> dataset, TableRef table, WriteMode mode, Options options);

    /** Writes with no options. */
    default void write(Dataset<Row> dataset, TableRef table, WriteMode mode) {
        write(dataset, table, mode, Options.none());
    }

    /** Appends with no options. */
    default void append(Dataset<Row> dataset, TableRef table) {
        write(dataset, table, WriteMode.APPEND, Options.none());
    }

    /** Replaces everything at {@code table} with these rows. */
    default void overwrite(Dataset<Row> dataset, TableRef table) {
        write(dataset, table, WriteMode.OVERWRITE, Options.none());
    }
}
