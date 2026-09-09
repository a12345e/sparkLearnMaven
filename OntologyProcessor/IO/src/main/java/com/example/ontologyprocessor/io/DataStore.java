package com.example.ontologyprocessor.io;

/**
 * A storage backend: everything this project asks of one place data lives.
 *
 * <p>{@code DataStore} is only the three contracts gathered up -
 * {@link DataReader}, {@link DataWriter}, {@link DdlOperations} - so that a
 * backend is one object to build, configure and hand around, while a caller
 * that needs less can still ask for less. Take a {@code DataReader} in a
 * method that only reads; take a {@code DataStore} where a whole backend is
 * genuinely the subject.
 *
 * <p>The implementations this is written for:
 *
 * <ul>
 *   <li>{@code Hadood} - HDFS and a Hive metastore, reached through Spark's
 *       Hive catalog, configured by {@code ClusterConfig}.</li>
 *   <li>Elasticsearch, later - indices instead of tables, no namespaces, and
 *       {@link WriteMode#OVERWRITE} implemented by recreating an index rather
 *       than by handing a mode to Spark.</li>
 * </ul>
 *
 * <p>Neither exists here, and neither should. This module is the contract; an
 * implementation lives in the module that owns the client library for it, and
 * depends on {@code com.example:IO}. Nothing in this package may ever depend
 * on a backend, or the two would drag each other onto every classpath.
 *
 * <p>{@link DdlOperations} is not {@code Serializable} on its own, but a
 * {@code DataStore} is, through the two data contracts. Nothing changes about
 * where DDL belongs: issue it from the driver. The interface simply cannot
 * un-inherit what it gathers.
 *
 * <p>Implementations are expected to be immutable and safe to share, and to
 * hold a connection or session lazily rather than in a constructor, so one can
 * be built in a test without a live cluster.
 */
public interface DataStore extends DataReader, DataWriter, DdlOperations {

    /**
     * @return a short name for this backend - {@code "hive"}, {@code "elasticsearch"} -
     *         used in log lines and error messages so a failure says which store it came from
     */
    String name();
}
