package infra.docker;

import java.io.File;

/**
 * The HDFS + Hive metastore stack in {@code docker/hadoop313hive313}.
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
    /** DataNode HTTP UI. The data transfer port, 9866, is deliberately not published; see startHint(). */
    public static final int DATANODE_HTTP_PORT = 9864;

    private static final long START_TIMEOUT_MS = 5 * 60 * 1000L;

    private final File composeDir;

    private HadoopStack(File composeDir) {
        this.composeDir = composeDir;
    }

    public static HadoopStack at(File projectRoot) {
        File dir = new File(projectRoot, "docker/hadoop313hive313");
        if (!new File(dir, "docker-compose.yml").isFile()) {
            throw new IllegalStateException("no docker-compose.yml in " + dir.getAbsolutePath());
        }
        return new HadoopStack(dir);
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
     * @return whether a locally running Spark can write HDFS <em>blocks</em>,
     *         as opposed to merely talking to the namenode and the metastore
     */
    public boolean canReachDataNode() {
        return Docker.tcpOpen("datanode", 9866, 1500) || Docker.tcpOpen("localhost", 9866, 1500);
    }

    /** @return what to change so {@link #canReachDataNode()} becomes true */
    public static String dataNodeHint() {
        return "The DataNode publishes only " + DATANODE_HTTP_PORT + " (its UI), and it registers "
                + "itself under the hostname 'datanode', which does not resolve on the host. A local "
                + "Spark can therefore reach the NameNode and the metastore but cannot read or write "
                + "HDFS blocks. To fix it: publish \"9866:9866\" on the datanode service, and add "
                + "'127.0.0.1 datanode' to the Windows hosts file so the name resolves both from the "
                + "host and, via docker's own DNS, from inside the other containers.";
    }
}
