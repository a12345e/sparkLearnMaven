package infra.docker;

import java.io.File;
import java.util.Properties;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The HDFS + Hive metastore stack packaged with this module.
 *
 * <p>Its compose file ships in the jar; {@link DockerAssets} writes it to disk
 * on first use, so a project depending on {@code com.example:Infra} needs
 * nothing checked out.
 *
 * <p>Spark runs locally and reaches this over published ports, so "is it up"
 * is answered by asking whether the ports are listening rather than by asking
 * docker what it thinks. That is the thing that actually has to be true.
 */
public final class HadoopStack {

    /** {@code fs.defaultFS}. */
    public static final int NAMENODE_RPC_PORT = 8020;
    /** The Hive metastore thrift port. */
    public static final int METASTORE_PORT = 9083;
    /** DataNode HTTP UI. */
    public static final int DATANODE_HTTP_PORT = 9864;

    /** The Hive this stack runs, matching hive.version in the parent POM. */
    public static final String HIVE_VERSION = "3.1.3";
    /** DataNode block transfer. Published, so a Spark job here can move blocks; see {@link #sparkHadoopOptions()}. */
    public static final int DATANODE_DATA_PORT = 9866;

    private static final long START_TIMEOUT_MS = 5 * 60 * 1000L;

    private final File composeDir;

    private HadoopStack(File composeDir) {
        this.composeDir = composeDir;
    }

    /** The stack packaged in this jar. */
    public static HadoopStack packaged() {
        return at(DockerAssets.hadoopStack());
    }

    /** A stack whose compose file you keep yourself. */
    public static HadoopStack at(File stackDir) {
        if (!new File(stackDir, "docker-compose.yml").isFile()) {
            throw new IllegalStateException("no docker-compose.yml in " + stackDir.getAbsolutePath());
        }
        return new HadoopStack(stackDir);
    }

    /** @return whether both the namenode and the metastore are accepting connections */
    public boolean isUp() {
        return Docker.tcpOpen("localhost", NAMENODE_RPC_PORT, 2000)
                && Docker.tcpOpen("localhost", METASTORE_PORT, 2000);
    }

    /**
     * Starts the stack unless it is already serving.
     *
     * @return whether it had to be started
     */
    public boolean ensureUp() {
        if (isUp()) {
            return false;
        }
        Docker.run(composeDir, "docker", "compose", "up", "-d", "--wait");
        Docker.await("the hadoop/hive stack", START_TIMEOUT_MS, new Docker.Ready() {
            @Override
            public boolean isReady() {
                return isUp();
            }
        });
        return true;
    }

    public String hdfsUri() {
        return "hdfs://localhost:" + NAMENODE_RPC_PORT;
    }

    public String metastoreUris() {
        return "thrift://localhost:" + METASTORE_PORT;
    }

    /**
     * The Hadoop settings a Spark session <em>on this machine</em> needs before
     * it can do more than talk to the NameNode. Keys are already prefixed with
     * {@code spark.hadoop.}, so they go straight onto a session builder:
     *
     * <pre>{@code
     * SparkSession.Builder builder = SparkSession.builder();
     * for (Map.Entry<String, String> option : hadoop.sparkHadoopOptions().entrySet()) {
     *     builder.config(option.getKey(), option.getValue());
     * }
     * }</pre>
     *
     * <p>Two settings, and the second is the one that is easy to miss.
     *
     * <p>{@code fs.defaultFS} is what makes a bare path such as {@code /tmp/x}
     * mean HDFS rather than the local disk. Without it every HDFS path has to
     * be written out in full as {@code hdfs://localhost:8020/tmp/x}, which
     * works but silently sends anything you forget to prefix to the local
     * filesystem instead.
     *
     * <p>{@code dfs.client.use.datanode.hostname} is what makes block IO work
     * at all. Reading a file is two conversations: the NameNode says which
     * DataNodes hold the blocks, then the client opens a socket to one of them.
     * The NameNode answers with both the DataNode's internal docker IP and the
     * name it registered under, and this flag picks which one the client dials.
     * Left false a client here would try {@code 172.x.y.z:9866}, which is not
     * routable from Windows, and the read would hang and then fail with "could
     * only be replicated to 0 nodes" or a socket timeout - <em>after</em> the
     * NameNode call succeeded, which is why the failure looks like a puzzle.
     * Set true it dials the registered name, which the stack sets to
     * {@code localhost} (see {@code DATANODE_ADVERTISED_HOST} in the compose
     * {@code .env}), and that resolves to the published {@value #DATANODE_DATA_PORT}.
     *
     * @return the settings, in the order they are worth reading
     */
    public Map<String, String> sparkHadoopOptions() {
        Map<String, String> options = new LinkedHashMap<String, String>();
        options.put("spark.hadoop.fs.defaultFS", hdfsUri());
        options.put("spark.hadoop.dfs.client.use.datanode.hostname", "true");
        return Collections.unmodifiableMap(options);
    }

    /**
     * The stack described the way {@code ClusterConfig} reads a properties
     * file, so a test can build the same configuration object production
     * builds from an external file:
     *
     * <pre>{@code
     * ClusterConfig cluster = ClusterConfig.of(hadoop.clusterProperties(), "docker");
     * }</pre>
     *
     * <p>The keys are literal strings rather than imported constants on
     * purpose: this module must not depend on {@code Hadood}, or {@code
     * Hadood} could not depend on this one from its own tests. The coupling is
     * to the documented key names, which is the same contract the external
     * properties file has.
     *
     * @return properties naming this stack, for {@code ClusterConfig.of}
     */
    public Properties clusterProperties() {
        Properties properties = new Properties();
        properties.setProperty("hdfs.uri", hdfsUri());
        properties.setProperty("hive.metastore.uris", metastoreUris());
        // Spark embeds Hive 2.3.9 and can build clients up to 3.1, so the
        // version this project targets has to come from somewhere other than
        // the classpath; "maven" lets Spark fetch it.
        properties.setProperty("hive.metastore.version", HIVE_VERSION);
        properties.setProperty("hive.metastore.jars", "maven");
        // Reaches the executors as spark.hadoop.*; without it block IO dials an
        // address on the docker network that this machine cannot route to.
        properties.setProperty("hadoop.opt.dfs.client.use.datanode.hostname", "true");
        return properties;
    }

    /**
     * @return whether a locally running Spark can write HDFS <em>blocks</em>,
     *         as opposed to merely talking to the namenode and the metastore
     */
    public boolean canReachDataNode() {
        return Docker.tcpOpen("localhost", DATANODE_DATA_PORT, 1500);
    }

    /** @return what to check when {@link #canReachDataNode()} is false */
    public static String dataNodeHint() {
        return "Nothing is listening on localhost:" + DATANODE_DATA_PORT + ", so HDFS blocks cannot be "
                + "read or written from here - the namenode and the metastore will still answer, which "
                + "makes this look like a Spark problem rather than a networking one. The datanode "
                + "service should publish \"" + DATANODE_DATA_PORT + ":" + DATANODE_DATA_PORT + "\"; "
                + "run `docker compose up -d --force-recreate datanode` in " + DockerAssets.HADOOP_STACK + " to "
                + "pick that up. Note that `docker compose restart datanode` is not enough and in fact "
                + "leaves it crash-looping on a stale pid file - it has to be recreated.";
    }
}
