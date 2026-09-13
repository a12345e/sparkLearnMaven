package com.example.ontologyprocessor.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.Serializable;

/**
 * Reading: a {@link TableRef} in, a {@code Dataset} out.
 *
 * <p>The same shape as {@code OntologyExtract}'s {@code Extractor}
 * ({@code SparkSession -> Dataset}), with the source named rather than baked
 * in. That is the whole difference, and the reason this package exists: an
 * extractor knows which table it reads, a reader is told, so one reader serves
 * every table in a backend and a caller can be pointed at another backend
 * without being rewritten.
 *
 * <p>Adapting one to the other is a lambda, which is why neither module needs
 * to depend on the other:
 *
 * <pre>{@code
 * Extractor people = spark -> store.read(spark, TableRef.parse("ontology.people"));
 * }</pre>
 *
 * <h2>Laziness</h2>
 *
 * <p>A read is lazy, like an extractor: it describes work without running it,
 * so a whole pipeline stays one Spark plan. Do not {@code count} or
 * {@code collect} inside an implementation. Resolving the schema is allowed -
 * a backend usually has to ask the catalog what the columns are - but reading
 * rows is not.
 *
 * <p>{@link Serializable}, since an implementation captured in a lambda is
 * shipped to the executors.
 */
public interface DataReader extends Serializable {

    /**
     * Reads everything at {@code table}.
     *
     * @param spark   the session to read with, never {@code null}
     * @param table   what to read, never {@code null}
     * @param options backend-specific settings; {@link Options#none()} for none
     * @return the rows, lazily
     * @throws IoException if the table cannot be read
     */
    Dataset<Row> read(SparkSession spark, TableRef table, Options options);

    /** Reads with no options. */
    default Dataset<Row> read(SparkSession spark, TableRef table) {
        return read(spark, table, Options.none());
    }
}
