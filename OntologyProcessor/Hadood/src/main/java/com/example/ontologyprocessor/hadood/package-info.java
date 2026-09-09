/**
 * The Hadoop stage: the external HDFS and Hive system, and how this project is
 * pointed at it.
 *
 * <p>{@link com.example.ontologyprocessor.hadood.ClusterConfig} is the whole
 * contract. It reads an external properties file, named by the
 * {@code hadood.config} system property or the {@code HADOOD_CONFIG}
 * environment variable, and turns it into a Hadoop {@code Configuration} and a
 * set of Spark settings. No hostname, port, path or credential is compiled in,
 * so the same jar runs against every environment.
 *
 * <p>{@code Hadood/conf/cluster.properties.example} is the annotated template.
 *
 * <p>The versions of the external system are declared once, in the parent POM:
 * Hadoop/HDFS 3.1.3 and Hive 3.1.3.
 *
 * <p>The module is independent of {@code OntologyExtract},
 * {@code OntologyTransform} and {@code OntologyLoad}; do not introduce a
 * dependency on them. A pipeline is assembled by a caller, not by a
 * compile-time chain.
 *
 * <p>Extend the module own {@code SparkTestSupport} to get the shared session,
 * fixture builders and assertions in scope. Nothing here needs a live cluster
 * to test: the file-to-settings mapping is pure, and that is where the
 * behaviour worth specifying lives.
 */
package com.example.ontologyprocessor.hadood;
