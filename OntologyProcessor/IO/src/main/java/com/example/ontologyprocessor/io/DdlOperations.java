package com.example.ontologyprocessor.io;

import org.apache.spark.sql.types.StructType;

import java.util.List;

/**
 * Structure rather than data: creating, dropping and inspecting namespaces and
 * tables.
 *
 * <p>Kept apart from {@link DataReader} and {@link DataWriter} because it is a
 * different job with a different lifetime. Reads and writes run per batch, on
 * a structure that already exists; DDL runs when the shape of the ontology
 * changes. Splitting them means a caller that only moves rows cannot
 * accidentally be handed something that drops a table, and a backend that is
 * read-only can implement half the contract honestly.
 *
 * <h2>Vocabulary</h2>
 *
 * <p>A <em>namespace</em> is a Hive database, or nothing at all on
 * Elasticsearch. A backend without namespaces should report
 * {@link #namespaceExists(String)} as {@code false} for every name, treat
 * {@link #createNamespace} as a no-op, and return an empty
 * {@link #listNamespaces()} rather than throw - a caller doing
 * "create the namespace, then the table" should not have to know which kind of
 * store it is talking to.
 *
 * <h2>Driver-side</h2>
 *
 * <p>Unlike the two data contracts this one is deliberately <em>not</em>
 * {@code Serializable}: DDL is issued once, from the driver, against a
 * catalog. Nothing here belongs inside a lambda on an executor, and not
 * declaring it serializable says so.
 *
 * <h2>What is not here</h2>
 *
 * <p>No {@code ALTER}. Hive and Elasticsearch disagree too deeply about what
 * changing a live schema means - one rewrites metadata, the other often needs
 * a reindex - and inventing a common answer before either implementation needs
 * one would be guesswork. Drop and recreate, or add the operation when a real
 * backend forces the question.
 *
 * <p>Every operation throws {@link IoException} when the backend fails. The
 * {@code exists} queries answer {@code false} for something absent; that is
 * not a failure.
 */
public interface DdlOperations {

    // ------------------------------------------------------------- namespaces

    /** @return whether the namespace exists; {@code false} on a backend without namespaces */
    boolean namespaceExists(String namespace);

    /**
     * Creates a namespace, doing nothing if it already exists, and nothing at
     * all on a backend that has no namespaces.
     *
     * @param options backend-specific settings, such as a Hive database location
     */
    void createNamespace(String namespace, Options options);

    /** Creates a namespace with no options. */
    default void createNamespace(String namespace) {
        createNamespace(namespace, Options.none());
    }

    /**
     * Drops a namespace, doing nothing if it is already absent.
     *
     * @param cascade whether to drop the tables in it; without it, a
     *                non-empty namespace is an {@link IoException}
     */
    void dropNamespace(String namespace, boolean cascade);

    /** @return every namespace, or empty on a backend without them */
    List<String> listNamespaces();

    // ----------------------------------------------------------------- tables

    /** @return whether the table exists */
    boolean tableExists(TableRef table);

    /**
     * Creates the table described by {@code spec}.
     *
     * @throws IoException if it already exists; use {@link #createTableIfAbsent(TableSpec)}
     *         when that is acceptable
     */
    void createTable(TableSpec spec);

    /** Creates the table unless it is already there. */
    default void createTableIfAbsent(TableSpec spec) {
        if (!tableExists(spec.table())) {
            createTable(spec);
        }
    }

    /** Drops the table, doing nothing if it is already absent. */
    void dropTable(TableRef table);

    /**
     * Removes every row, leaving the table and its schema.
     *
     * @throws IoException if the table does not exist
     */
    void truncateTable(TableRef table);

    /**
     * @param namespace the namespace to list, or {@code null} for a backend without them
     * @return the tables there, or empty
     */
    List<TableRef> listTables(String namespace);

    /**
     * @return the table's columns and types
     * @throws IoException if the table does not exist
     */
    StructType schemaOf(TableRef table);
}
