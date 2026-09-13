package com.example.ontologyprocessor.hadood;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;

/**
 * The external Hadoop/HDFS/Hive system this module talks to, described by a
 * configuration file <em>outside</em> the build.
 *
 * <p>Nothing about the cluster is compiled in. The file is found via the
 * {@code hadood.config} system property or the {@code HADOOD_CONFIG}
 * environment variable, so one jar runs against dev, staging and production:
 *
 * <pre>{@code
 * spark-submit --conf spark.driver.extraJavaOptions=-Dhadood.config=/etc/ontology/cluster.properties ...
 * }</pre>
 *
 * <p>{@code Hadood/conf/cluster.properties.example} documents every key. The
 * two that matter:
 *
 * <pre>{@code
 * hdfs.uri            = hdfs://namenode.example.com:8020
 * hive.metastore.uris = thrift://metastore.example.com:9083
 * }</pre>
 *
 * <p>From there:
 *
 * <pre>{@code
 * ClusterConfig cluster = ClusterConfig.load();
 * cluster.login();
 * SparkSession spark = cluster.configure(SparkSession.builder().appName("ontology")).getOrCreate();
 * }</pre>
 *
 * <h2>Hive 3.1.3</h2>
 *
 * <p>Spark 3.5 embeds Hive 2.3.9 for its own internals and cannot simply be
 * handed other Hive jars on the classpath. It reads a Hive 3.1.3 metastore by
 * loading that client in an isolated classloader, which is what
 * {@code spark.sql.hive.metastore.version} and
 * {@code spark.sql.hive.metastore.jars} select, so a config naming a metastore
 * must also say where to load the client from - a jars path, or
 * {@code hive.metastore.jars = maven} to let Spark fetch it.
 * {@link #sparkSettings()} enforces that rather than letting Spark fail later
 * on a version mismatch.
 *
 * <p>3.1.3 is not an arbitrary choice: Spark 3.5 ships isolated clients for
 * Hive 0.12 through 3.1 and nothing above, so a higher version here fails at
 * session start however the jars are configured. A newer metastore *server*
 * is fine - it is the client Spark has to be able to build.
 *
 * <p>Instances are immutable; the file is read once, in {@link #load(File)}.
 */
public final class ClusterConfig {

    /** System property naming the configuration file. */
    public static final String LOCATION_PROPERTY = "hadood.config";

    /** Environment variable naming the configuration file, if the property is unset. */
    public static final String LOCATION_ENV = "HADOOD_CONFIG";

    /**
     * The Hive version this project targets; the default for
     * {@link #HIVE_METASTORE_VERSION}.
     *
     * <p>Also the highest Spark 3.5 can load: its isolated client loader knows
     * v12 through v3_1.
     */
    public static final String HIVE_VERSION = "3.1.3";

    /** The Hive version Spark 3.5 embeds. Anything else needs external jars. */
    static final String SPARK_BUILTIN_HIVE_VERSION = "2.3.9";

    // ------------------------------------------------------------------- keys

    /** {@code hdfs://host:port} of the external namenode. */
    public static final String HDFS_URI = "hdfs.uri";
    /** Directory holding the cluster core-site.xml and hdfs-site.xml. */
    public static final String HADOOP_CONF_DIR = "hadoop.conf.dir";
    /** User to act as on an unsecured cluster. */
    public static final String HADOOP_USER_NAME = "hadoop.user.name";
    /** Directory holding the cluster hive-site.xml. */
    public static final String HIVE_CONF_DIR = "hive.conf.dir";
    /** {@code thrift://host:port} of the external Hive metastore. */
    public static final String HIVE_METASTORE_URIS = "hive.metastore.uris";
    /** Metastore version to speak; defaults to {@link #HIVE_VERSION}. */
    public static final String HIVE_METASTORE_VERSION = "hive.metastore.version";
    /** How Spark finds the metastore client: {@code builtin}, {@code path} or {@code maven}. */
    public static final String HIVE_METASTORE_JARS = "hive.metastore.jars";
    /** Where the Hive client jars live when {@link #HIVE_METASTORE_JARS} is {@code path}. */
    public static final String HIVE_METASTORE_JARS_PATH = "hive.metastore.jars.path";
    /** Hive warehouse root on the external cluster. */
    public static final String HIVE_WAREHOUSE_DIR = "hive.warehouse.dir";
    /** {@code simple} or {@code kerberos}. */
    public static final String SECURITY_AUTHENTICATION = "security.authentication";
    /** Kerberos principal to log in as. */
    public static final String KERBEROS_PRINCIPAL = "kerberos.principal";
    /** Keytab for {@link #KERBEROS_PRINCIPAL}. */
    public static final String KERBEROS_KEYTAB = "kerberos.keytab";

    /** Prefix for raw Hadoop settings: {@code hadoop.opt.dfs.replication=2}. */
    public static final String HADOOP_OPT_PREFIX = "hadoop.opt.";
    /** Keys with this prefix reach Spark verbatim. */
    public static final String SPARK_PREFIX = "spark.";

    private static final String CORE_SITE_FILE = "core-site.xml";
    private static final String[] HADOOP_SITE_FILES = {CORE_SITE_FILE, "hdfs-site.xml", "yarn-site.xml"};
    private static final String HIVE_SITE_FILE = "hive-site.xml";

    private final Map<String, String> settings;
    private final String origin;

    private ClusterConfig(Map<String, String> settings, String origin) {
        this.settings = Collections.unmodifiableMap(settings);
        this.origin = origin;
    }

    // ------------------------------------------------------------------- load

    /**
     * Loads the configuration named by {@link #LOCATION_PROPERTY} or
     * {@link #LOCATION_ENV}.
     *
     * @throws IllegalStateException if neither is set, or the file is missing
     */
    public static ClusterConfig load() {
        return load(locate());
    }

    /** Loads a specific configuration file. */
    public static ClusterConfig load(File file) {
        if (file == null) {
            throw new IllegalArgumentException("file must not be null");
        }
        if (!file.isFile()) {
            throw new IllegalStateException("cluster configuration file not found: " + file.getAbsolutePath());
        }
        Properties properties = new Properties();
        InputStream in = null;
        try {
            in = new FileInputStream(file);
            properties.load(in);
        } catch (IOException e) {
            throw new UncheckedIOException("could not read cluster configuration " + file.getAbsolutePath(), e);
        } finally {
            closeQuietly(in);
        }
        return of(properties, file.getAbsolutePath());
    }

    /** Builds a configuration from properties already in hand; {@code origin} appears in error messages. */
    public static ClusterConfig of(Properties properties, String origin) {
        if (properties == null) {
            throw new IllegalArgumentException("properties must not be null");
        }
        Map<String, String> settings = new LinkedHashMap<String, String>();
        for (String name : new TreeSet<String>(properties.stringPropertyNames())) {
            String value = properties.getProperty(name);
            if (value != null && !value.trim().isEmpty()) {
                settings.put(name.trim(), value.trim());
            }
        }
        return new ClusterConfig(settings, origin == null ? "<in memory>" : origin);
    }

    /** @return the file named by the system property or the environment variable */
    public static File locate() {
        String location = System.getProperty(LOCATION_PROPERTY);
        if (isBlank(location)) {
            location = System.getenv(LOCATION_ENV);
        }
        if (isBlank(location)) {
            throw new IllegalStateException("no cluster configuration: set -D" + LOCATION_PROPERTY
                    + "=<file> or " + LOCATION_ENV + "=<file>");
        }
        return new File(location.trim());
    }

    // ----------------------------------------------------------------- access

    /** @return where this configuration was read from, for error messages */
    public String origin() {
        return origin;
    }

    /** @return the value, or {@code null} if absent or blank */
    public String get(String key) {
        return settings.get(key);
    }

    public String get(String key, String defaultValue) {
        String value = settings.get(key);
        return value == null ? defaultValue : value;
    }

    /** @throws IllegalStateException naming the file, if the key is absent */
    public String require(String key) {
        String value = settings.get(key);
        if (value == null) {
            throw new IllegalStateException("missing " + quote(key) + " in " + origin);
        }
        return value;
    }

    /** @return every key present, in sorted order */
    public Set<String> keys() {
        return Collections.unmodifiableSet(new TreeSet<String>(settings.keySet()));
    }

    // ----------------------------------------------------------------- hadoop

    /**
     * Builds a Hadoop {@link Configuration} for the external cluster: the site
     * XML files named by {@link #HADOOP_CONF_DIR} and {@link #HIVE_CONF_DIR},
     * with this file own settings on top.
     */
    public Configuration hadoopConfiguration() {
        Configuration conf = new Configuration();
        for (String siteFile : HADOOP_SITE_FILES) {
            addSiteFile(conf, HADOOP_CONF_DIR, siteFile, !CORE_SITE_FILE.equals(siteFile));
        }
        addSiteFile(conf, HIVE_CONF_DIR, HIVE_SITE_FILE, true);

        set(conf, "fs.defaultFS", get(HDFS_URI));
        set(conf, "hadoop.security.authentication", get(SECURITY_AUTHENTICATION));
        set(conf, "hive.metastore.uris", get(HIVE_METASTORE_URIS));
        set(conf, "hive.metastore.warehouse.dir", get(HIVE_WAREHOUSE_DIR));
        set(conf, "hive.metastore.kerberos.principal", get(KERBEROS_PRINCIPAL));

        for (Map.Entry<String, String> override : hadoopOverrides().entrySet()) {
            conf.set(override.getKey(), override.getValue());
        }
        return conf;
    }

    /** @return the raw Hadoop settings given as {@code hadoop.opt.*}, prefix stripped */
    public Map<String, String> hadoopOverrides() {
        Map<String, String> overrides = new LinkedHashMap<String, String>();
        for (Map.Entry<String, String> entry : settings.entrySet()) {
            if (entry.getKey().startsWith(HADOOP_OPT_PREFIX)) {
                String key = entry.getKey().substring(HADOOP_OPT_PREFIX.length());
                if (!key.isEmpty()) {
                    overrides.put(key, entry.getValue());
                }
            }
        }
        return overrides;
    }

    /** @return the external cluster default filesystem */
    public FileSystem fileSystem() {
        try {
            return FileSystem.get(hadoopConfiguration());
        } catch (IOException e) {
            throw new UncheckedIOException("could not reach " + get(HDFS_URI, "the default filesystem"), e);
        }
    }

    /**
     * Authenticates this JVM against the external cluster.
     *
     * <p>Kerberos when a principal and keytab are configured, otherwise the
     * configured user name. Call once, before touching HDFS or Hive.
     *
     * @throws IllegalStateException if the cluster is secured but no keytab was configured
     */
    public void login() {
        Configuration conf = hadoopConfiguration();
        UserGroupInformation.setConfiguration(conf);

        String principal = get(KERBEROS_PRINCIPAL);
        String keytab = get(KERBEROS_KEYTAB);
        if (principal != null && keytab != null) {
            try {
                UserGroupInformation.loginUserFromKeytab(principal, keytab);
            } catch (IOException e) {
                throw new UncheckedIOException("kerberos login failed for " + principal, e);
            }
            return;
        }
        if (UserGroupInformation.isSecurityEnabled()) {
            throw new IllegalStateException("cluster is kerberised: set " + quote(KERBEROS_PRINCIPAL)
                    + " and " + quote(KERBEROS_KEYTAB) + " in " + origin);
        }
        String user = get(HADOOP_USER_NAME);
        if (user != null) {
            System.setProperty("HADOOP_USER_NAME", user);
        }
    }

    // ------------------------------------------------------------------ spark

    /**
     * Applies {@link #sparkSettings()} to a builder, enabling Hive support when
     * a metastore is configured.
     *
     * @return the same builder, for chaining
     */
    public SparkSession.Builder configure(SparkSession.Builder builder) {
        if (builder == null) {
            throw new IllegalArgumentException("builder must not be null");
        }
        for (Map.Entry<String, String> setting : sparkSettings().entrySet()) {
            builder.config(setting.getKey(), setting.getValue());
        }
        if (get(HIVE_METASTORE_URIS) != null) {
            builder.enableHiveSupport();
        }
        return builder;
    }

    /**
     * Translates this configuration into Spark settings.
     *
     * <p>Pure: it starts no session and touches no cluster, which is what makes
     * the mapping testable. Hadoop settings are passed as {@code spark.hadoop.*}
     * so they reach the executors too.
     *
     * @throws IllegalStateException if a non-builtin metastore version is
     *         configured without jars to load it from
     */
    public Map<String, String> sparkSettings() {
        Map<String, String> spark = new LinkedHashMap<String, String>();

        putHadoop(spark, "fs.defaultFS", get(HDFS_URI));
        putHadoop(spark, "hadoop.security.authentication", get(SECURITY_AUTHENTICATION));
        putHadoop(spark, "hive.metastore.kerberos.principal", get(KERBEROS_PRINCIPAL));
        for (Map.Entry<String, String> override : hadoopOverrides().entrySet()) {
            putHadoop(spark, override.getKey(), override.getValue());
        }

        String metastore = get(HIVE_METASTORE_URIS);
        if (metastore != null) {
            putHadoop(spark, "hive.metastore.uris", metastore);
            spark.putAll(metastoreClientSettings());
        }

        String warehouse = get(HIVE_WAREHOUSE_DIR);
        if (warehouse != null) {
            spark.put("spark.sql.warehouse.dir", warehouse);
        }

        // Verbatim last, so an explicit spark.* key always wins.
        for (Map.Entry<String, String> entry : settings.entrySet()) {
            if (entry.getKey().startsWith(SPARK_PREFIX)) {
                spark.put(entry.getKey(), entry.getValue());
            }
        }
        return spark;
    }

    @Override
    public String toString() {
        return "ClusterConfig(" + origin + ", " + settings.size() + " setting(s))";
    }

    // ---------------------------------------------------------------- helpers

    /**
     * Chooses the isolated classloader Spark loads the metastore client with,
     * and refuses the combinations Spark would only reject once it connected.
     */
    private Map<String, String> metastoreClientSettings() {
        String version = get(HIVE_METASTORE_VERSION, HIVE_VERSION);
        String jarsPath = get(HIVE_METASTORE_JARS_PATH);
        String jars = get(HIVE_METASTORE_JARS, jarsPath == null ? "builtin" : "path");

        if ("builtin".equals(jars) && !SPARK_BUILTIN_HIVE_VERSION.equals(version)) {
            throw new IllegalStateException("Spark embeds Hive " + SPARK_BUILTIN_HIVE_VERSION
                    + " and cannot speak Hive " + version + " with " + quote(HIVE_METASTORE_JARS)
                    + "=builtin. Set " + quote(HIVE_METASTORE_JARS_PATH) + " to the Hive " + version
                    + " client jars in " + origin);
        }
        if ("path".equals(jars) && jarsPath == null) {
            throw new IllegalStateException(quote(HIVE_METASTORE_JARS) + "=path needs "
                    + quote(HIVE_METASTORE_JARS_PATH) + " in " + origin);
        }

        Map<String, String> spark = new LinkedHashMap<String, String>();
        spark.put("spark.sql.hive.metastore.version", version);
        spark.put("spark.sql.hive.metastore.jars", jars);
        if (jarsPath != null) {
            spark.put("spark.sql.hive.metastore.jars.path", jarsPath);
        }
        return spark;
    }

    private void addSiteFile(Configuration conf, String dirKey, String fileName, boolean optionalFile) {
        String dir = get(dirKey);
        if (dir == null) {
            return;
        }
        File directory = new File(dir);
        if (!directory.isDirectory()) {
            throw new IllegalStateException(quote(dirKey) + " is not a directory: "
                    + directory.getAbsolutePath() + " (from " + origin + ")");
        }
        File siteFile = new File(directory, fileName);
        if (siteFile.isFile()) {
            conf.addResource(new Path(siteFile.toURI()));
        } else if (!optionalFile) {
            throw new IllegalStateException("no " + fileName + " in " + quote(dirKey) + " "
                    + directory.getAbsolutePath() + " (from " + origin + ")");
        }
    }

    private static void putHadoop(Map<String, String> spark, String key, String value) {
        if (value != null) {
            spark.put("spark.hadoop." + key, value);
        }
    }

    private static void set(Configuration conf, String key, String value) {
        if (value != null) {
            conf.set(key, value);
        }
    }

    private static String quote(String key) {
        return "'" + key + "'";
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static void closeQuietly(InputStream in) {
        if (in != null) {
            try {
                in.close();
            } catch (IOException ignored) {
                // Nothing useful to do; the read already succeeded or failed.
            }
        }
    }
}
