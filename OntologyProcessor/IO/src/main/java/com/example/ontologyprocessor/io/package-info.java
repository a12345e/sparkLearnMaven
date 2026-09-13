/**
 * The IO contracts: how this project reads rows, writes rows and manages
 * structure, stated once so that every storage backend states it the same way.
 *
 * <p>Nothing here talks to anything. The module ships interfaces and the small
 * value types they are written in, and depends on Spark and nothing else -
 * no Hadoop, no Hive, no Elasticsearch client. An implementation lives in the
 * module that owns its client library and depends on {@code com.example:IO};
 * the dependency never points the other way.
 *
 * <h2>The contracts</h2>
 *
 * <table border="1">
 *   <caption>The four interfaces</caption>
 *   <tr><th>Interface</th><th>Shape</th></tr>
 *   <tr><td>{@link com.example.ontologyprocessor.io.DataReader}</td>
 *       <td>{@code (SparkSession, TableRef) -> Dataset}</td></tr>
 *   <tr><td>{@link com.example.ontologyprocessor.io.DataWriter}</td>
 *       <td>{@code (Dataset, TableRef, WriteMode) -> void}</td></tr>
 *   <tr><td>{@link com.example.ontologyprocessor.io.DdlOperations}</td>
 *       <td>namespaces and tables: create, drop, exists, list, schema</td></tr>
 *   <tr><td>{@link com.example.ontologyprocessor.io.DataStore}</td>
 *       <td>all three, for a backend that is all three</td></tr>
 * </table>
 *
 * <p>They are separate so a caller can ask for exactly what it uses. A method
 * that only reads should take a {@code DataReader}: it then cannot drop a
 * table, and a read-only backend can satisfy it honestly.
 *
 * <h2>And the vocabulary</h2>
 *
 * <ul>
 *   <li>{@link com.example.ontologyprocessor.io.TableRef} - an optional
 *       namespace and a name. A Hive database and table, or an Elasticsearch
 *       index.</li>
 *   <li>{@link com.example.ontologyprocessor.io.WriteMode} - append,
 *       overwrite, fail, ignore.</li>
 *   <li>{@link com.example.ontologyprocessor.io.TableSpec} - a
 *       {@code TableRef}, a Spark {@code StructType} and partition columns:
 *       what {@code createTable} needs.</li>
 *   <li>{@link com.example.ontologyprocessor.io.Options} - an immutable string
 *       map for whatever only one backend understands.</li>
 *   <li>{@link com.example.ontologyprocessor.io.IoException} - unchecked; what
 *       a backend throws when it fails.</li>
 * </ul>
 *
 * <p>{@code TableRef}, {@code WriteMode} and {@code TableSpec} are the
 * portable half of a call and every backend must honour them.
 * {@code Options} is the escape hatch, and the only place backend knowledge is
 * allowed to appear. An implementation should ignore an option it does not
 * recognise rather than fail, so the same options can be handed to two stores.
 *
 * <h2>Relation to the ETL stages</h2>
 *
 * <p>{@code Extractor} and {@code Loader} are the same shapes with the source
 * and sink baked in; a reader and a writer are told where instead. Neither
 * module depends on the other, because bridging them is a lambda:
 *
 * <pre>{@code
 * Extractor people = spark   -> store.read(spark, TableRef.parse("ontology.people"));
 * Loader    counts = dataset -> store.write(dataset, TableRef.of("counts"), WriteMode.OVERWRITE);
 * }</pre>
 *
 * <p>Laziness carries over unchanged: a read is lazy, a write is the action
 * that runs the plan. See {@code DataReader} for what that rules out.
 *
 * <h2>Writing an implementation</h2>
 *
 * <p>Add {@code com.example:IO} to the backend module's {@code pom.xml} - the
 * version is managed by the parent - and implement {@link
 * com.example.ontologyprocessor.io.DataStore}. Then specify it against
 * {@code DataStoreContract} in this module's test tree, which is the same set
 * of tests the in-memory store passes: what a backend must do is written down
 * once, and each new backend inherits it rather than reinventing it.
 */
package com.example.ontologyprocessor.io;
