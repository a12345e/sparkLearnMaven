package com.example.ontologyprocessor.hadood;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Specifies how an external configuration file becomes Hadoop and Spark
 * settings.
 *
 * <p>No cluster is contacted: every assertion is about the mapping, which is
 * where the behaviour worth specifying lives. The one exception is
 * {@link #readsSiteXmlFromTheHadoopConfDirectory()}, which proves the
 * hadoop-hdfs 3.1.3 client on the classpath really parses the cluster site
 * files this module hands it.
 */
public class ClusterConfigTest extends SparkTestSupport {

    // ------------------------------------------------------------------- file

    @Test
    public void readsSettingsFromAnExternalFile() {
        ClusterConfig cluster = configFile(
                "hdfs.uri = hdfs://namenode.example.com:8020",
                "hadoop.user.name = ontology");

        assertEquals("hdfs://namenode.example.com:8020", cluster.get(ClusterConfig.HDFS_URI));
        assertEquals("ontology", cluster.get(ClusterConfig.HADOOP_USER_NAME));
        assertNull(cluster.get("hive.metastore.uris"));
    }

    @Test
    public void treatsABlankValueAsAbsent() {
        ClusterConfig cluster = configFile("kerberos.principal =", "hdfs.uri = hdfs://nn:8020");

        assertNull(cluster.get(ClusterConfig.KERBEROS_PRINCIPAL));
        assertEquals("fallback", cluster.get(ClusterConfig.KERBEROS_PRINCIPAL, "fallback"));
        assertFalse(cluster.keys().contains(ClusterConfig.KERBEROS_PRINCIPAL));
    }

    @Test
    public void namesTheFileWhenARequiredKeyIsMissing() {
        File file = write("hdfs.uri = hdfs://nn:8020");
        ClusterConfig cluster = ClusterConfig.load(file);

        try {
            cluster.require(ClusterConfig.HIVE_METASTORE_URIS);
            fail("expected a missing-key failure");
        } catch (IllegalStateException expected) {
            assertContains(expected.getMessage(), ClusterConfig.HIVE_METASTORE_URIS);
            assertContains(expected.getMessage(), file.getAbsolutePath());
        }
    }

    @Test
    public void refusesAMissingFile() {
        File missing = new File(tempDir("hadood-missing"), "nope.properties");
        try {
            ClusterConfig.load(missing);
            fail("expected a missing-file failure");
        } catch (IllegalStateException expected) {
            assertContains(expected.getMessage(), missing.getAbsolutePath());
        }
    }

    @Test
    public void saysHowToNameTheFileWhenNothingPointsAtOne() {
        String previous = System.getProperty(ClusterConfig.LOCATION_PROPERTY);
        System.clearProperty(ClusterConfig.LOCATION_PROPERTY);
        try {
            if (System.getenv(ClusterConfig.LOCATION_ENV) != null) {
                return; // The environment names one; nothing to assert here.
            }
            ClusterConfig.locate();
            fail("expected a missing-location failure");
        } catch (IllegalStateException expected) {
            assertContains(expected.getMessage(), ClusterConfig.LOCATION_PROPERTY);
            assertContains(expected.getMessage(), ClusterConfig.LOCATION_ENV);
        } finally {
            if (previous != null) {
                System.setProperty(ClusterConfig.LOCATION_PROPERTY, previous);
            }
        }
    }

    @Test
    public void locatesTheFileNamedBySystemProperty() {
        File file = write("hdfs.uri = hdfs://nn:8020");
        String previous = System.getProperty(ClusterConfig.LOCATION_PROPERTY);
        System.setProperty(ClusterConfig.LOCATION_PROPERTY, file.getAbsolutePath());
        try {
            assertEquals("hdfs://nn:8020", ClusterConfig.load().get(ClusterConfig.HDFS_URI));
        } finally {
            if (previous == null) {
                System.clearProperty(ClusterConfig.LOCATION_PROPERTY);
            } else {
                System.setProperty(ClusterConfig.LOCATION_PROPERTY, previous);
            }
        }
    }

    // ------------------------------------------------------------ spark: hdfs

    @Test
    public void carriesHdfsAndItsOverridesToTheExecutors() {
        Map<String, String> spark = configFile(
                "hdfs.uri = hdfs://namenode.example.com:8020",
                "security.authentication = kerberos",
                "hadoop.opt.dfs.client.use.datanode.hostname = true").sparkSettings();

        assertEquals("hdfs://namenode.example.com:8020", spark.get("spark.hadoop.fs.defaultFS"));
        assertEquals("kerberos", spark.get("spark.hadoop.hadoop.security.authentication"));
        assertEquals("true", spark.get("spark.hadoop.dfs.client.use.datanode.hostname"));
    }

    @Test
    public void passesSparkKeysThroughUntouchedAndLetsThemWin() {
        Map<String, String> spark = configFile(
                "hdfs.uri = hdfs://nn:8020",
                "spark.sql.sources.partitionOverwriteMode = dynamic",
                "spark.hadoop.fs.defaultFS = hdfs://override:8020").sparkSettings();

        assertEquals("dynamic", spark.get("spark.sql.sources.partitionOverwriteMode"));
        assertEquals("hdfs://override:8020", spark.get("spark.hadoop.fs.defaultFS"));
    }

    @Test
    public void staysSilentAboutWhatWasNotConfigured() {
        Map<String, String> spark = configFile("hdfs.uri = hdfs://nn:8020").sparkSettings();

        assertEquals("only fs.defaultFS: " + spark, 1, spark.size());
        assertFalse("no metastore configured", spark.containsKey("spark.sql.hive.metastore.version"));
    }

    // ------------------------------------------------------------ spark: hive

    @Test
    public void pointsSparkAtTheExternalHiveMetastore() {
        Map<String, String> spark = configFile(
                "hive.metastore.uris = thrift://metastore.example.com:9083",
                "hive.metastore.jars.path = file:///opt/hive-3.1.3/lib/*",
                "hive.warehouse.dir = hdfs://nn:8020/warehouse").sparkSettings();

        assertEquals("thrift://metastore.example.com:9083", spark.get("spark.hadoop.hive.metastore.uris"));
        assertEquals(ClusterConfig.HIVE_VERSION, spark.get("spark.sql.hive.metastore.version"));
        assertEquals("path", spark.get("spark.sql.hive.metastore.jars"));
        assertEquals("file:///opt/hive-3.1.3/lib/*", spark.get("spark.sql.hive.metastore.jars.path"));
        assertEquals("hdfs://nn:8020/warehouse", spark.get("spark.sql.warehouse.dir"));
    }

    @Test
    public void refusesTheTargetHiveVersionWithoutTheJarsToSpeakIt() {
        ClusterConfig cluster = configFile("hive.metastore.uris = thrift://metastore:9083");

        try {
            cluster.sparkSettings();
            fail("expected Hive " + ClusterConfig.HIVE_VERSION
                    + " against the embedded Hive 2.3.9 to be refused");
        } catch (IllegalStateException expected) {
            assertContains(expected.getMessage(), ClusterConfig.SPARK_BUILTIN_HIVE_VERSION);
            assertContains(expected.getMessage(), ClusterConfig.HIVE_METASTORE_JARS_PATH);
        }
    }

    @Test
    public void refusesAJarsPathModeWithNoPath() {
        ClusterConfig cluster = configFile(
                "hive.metastore.uris = thrift://metastore:9083",
                "hive.metastore.jars = path");

        try {
            cluster.sparkSettings();
            fail("expected the empty jars path to be refused");
        } catch (IllegalStateException expected) {
            assertContains(expected.getMessage(), ClusterConfig.HIVE_METASTORE_JARS_PATH);
        }
    }

    @Test
    public void allowsTheEmbeddedHiveWhenThatIsWhatWasAskedFor() {
        Map<String, String> spark = configFile(
                "hive.metastore.uris = thrift://metastore:9083",
                "hive.metastore.version = 2.3.9").sparkSettings();

        assertEquals("2.3.9", spark.get("spark.sql.hive.metastore.version"));
        assertEquals("builtin", spark.get("spark.sql.hive.metastore.jars"));
        assertFalse(spark.containsKey("spark.sql.hive.metastore.jars.path"));
    }

    // ----------------------------------------------------------- hadoop conf

    @Test
    public void readsSiteXmlFromTheHadoopConfDirectory() {
        File confDir = tempDir("hadood-conf");
        writeSiteXml(new File(confDir, "core-site.xml"), "fs.defaultFS", "hdfs://from-site-xml:8020");
        writeSiteXml(new File(confDir, "hdfs-site.xml"), "dfs.replication", "3");

        Configuration conf = configFile("hadoop.conf.dir = " + path(confDir)).hadoopConfiguration();

        assertEquals("hdfs://from-site-xml:8020", conf.get("fs.defaultFS"));
        assertEquals("3", conf.get("dfs.replication"));
    }

    @Test
    public void letsTheConfigFileOverrideTheClusterSiteXml() {
        File confDir = tempDir("hadood-conf");
        writeSiteXml(new File(confDir, "core-site.xml"), "fs.defaultFS", "hdfs://from-site-xml:8020");

        Configuration conf = configFile(
                "hadoop.conf.dir = " + path(confDir),
                "hdfs.uri = hdfs://from-config-file:8020",
                "hadoop.opt.dfs.replication = 2").hadoopConfiguration();

        assertEquals("hdfs://from-config-file:8020", conf.get("fs.defaultFS"));
        assertEquals("2", conf.get("dfs.replication"));
    }

    @Test
    public void carriesHiveSettingsIntoTheHadoopConfiguration() {
        Configuration conf = configFile(
                "hive.metastore.uris = thrift://metastore:9083",
                "hive.warehouse.dir = hdfs://nn:8020/warehouse").hadoopConfiguration();

        assertEquals("thrift://metastore:9083", conf.get("hive.metastore.uris"));
        assertEquals("hdfs://nn:8020/warehouse", conf.get("hive.metastore.warehouse.dir"));
    }

    @Test
    public void refusesAHadoopConfDirectoryThatIsNotThere() {
        File missing = new File(tempDir("hadood-conf"), "absent");
        ClusterConfig cluster = configFile("hadoop.conf.dir = " + path(missing));

        try {
            cluster.hadoopConfiguration();
            fail("expected the missing directory to be refused");
        } catch (IllegalStateException expected) {
            assertContains(expected.getMessage(), ClusterConfig.HADOOP_CONF_DIR);
        }
    }

    @Test
    public void refusesAHadoopConfDirectoryWithNoCoreSite() {
        File confDir = tempDir("hadood-conf");
        ClusterConfig cluster = configFile("hadoop.conf.dir = " + path(confDir));

        try {
            cluster.hadoopConfiguration();
            fail("expected the missing core-site.xml to be refused");
        } catch (IllegalStateException expected) {
            assertContains(expected.getMessage(), "core-site.xml");
        }
    }

    // ---------------------------------------------------------------- helpers

    /** Writes the lines to a fresh file outside the build and loads it. */
    private static ClusterConfig configFile(String... lines) {
        return ClusterConfig.load(write(lines));
    }

    private static File write(String... lines) {
        File file = new File(tempDir("hadood-config"), "cluster.properties");
        Writer writer = null;
        try {
            writer = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
            for (String line : lines) {
                writer.write(line);
                writer.write("\n");
            }
        } catch (IOException e) {
            throw new IllegalStateException("could not write " + file, e);
        } finally {
            close(writer);
        }
        return file;
    }

    private static void writeSiteXml(File file, String key, String value) {
        write(file, "<?xml version=\"1.0\"?><configuration><property><name>" + key
                + "</name><value>" + value + "</value></property></configuration>");
    }

    private static void write(File file, String content) {
        Writer writer = null;
        try {
            writer = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");
            writer.write(content);
        } catch (IOException e) {
            throw new IllegalStateException("could not write " + file, e);
        } finally {
            close(writer);
        }
    }

    /** Backslashes are escapes in a properties file, so a Windows path needs doubling. */
    private static String path(File file) {
        return file.getAbsolutePath().replace("\\", "\\\\");
    }

    private static void close(Writer writer) {
        if (writer == null) {
            return;
        }
        try {
            writer.close();
        } catch (IOException e) {
            throw new IllegalStateException("could not close the writer", e);
        }
    }

    private static void assertContains(String actual, String expected) {
        assertTrue("expected to find " + expected + " in: " + actual,
                actual != null && actual.contains(expected));
    }
}
