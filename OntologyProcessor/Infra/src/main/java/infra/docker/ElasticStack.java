package infra.docker;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * The Elasticsearch clusters packaged with this module.
 *
 * <p>Its compose file ships in the jar; {@link DockerAssets} writes it to disk
 * on first use.
 *
 * <p>N clusters means N compose projects named {@code es-1}, {@code es-2}, ...
 * off one compose file, which is how that stack is built: each project gets
 * its own network, volume and container names, and each cluster can then be
 * addressed by a distinct alias for remote reindex.
 *
 * <p>Host ports are ephemeral, so the URL is never assumed - it is read back
 * from {@code docker compose port} once the cluster is up.
 */
public final class ElasticStack {

    private static final String SERVICE = "es";
    private static final int ES_PORT = 9200;
    private static final String SHARED_NETWORK = "es-shared";
    private static final long START_TIMEOUT_MS = 3 * 60 * 1000L;

    private final File composeDir;

    private ElasticStack(File composeDir) {
        this.composeDir = composeDir;
    }

    /** The clusters packaged in this jar. */
    public static ElasticStack packaged() {
        return at(DockerAssets.elasticClusters());
    }

    /** Clusters whose compose file you keep yourself. */
    public static ElasticStack at(File composeDir) {
        if (!new File(composeDir, "docker-compose.yml").isFile()) {
            throw new IllegalStateException("no docker-compose.yml in " + composeDir.getAbsolutePath());
        }
        return new ElasticStack(composeDir);
    }

    /** @return the compose project name of the nth cluster, counting from 1 */
    public static String project(int cluster) {
        return "es-" + cluster;
    }

    /**
     * Starts clusters until {@code wanted} of them are running.
     *
     * <p>Idempotent, like the {@code clusters.ps1} it mirrors: already-running
     * clusters are left alone.
     *
     * @param wanted how many clusters, 1 to 6
     * @return their host URLs, in cluster order
     */
    public List<String> ensureUp(int wanted) {
        if (wanted < 1 || wanted > 6) {
            throw new IllegalArgumentException("wanted must be 1..6, not " + wanted);
        }
        ensureSharedNetwork();

        List<String> urls = new ArrayList<String>(wanted);
        for (int cluster = 1; cluster <= wanted; cluster++) {
            final String project = project(cluster);
            if (!isRunning(project)) {
                Docker.run(composeDir, Docker.env("CLUSTER_NAME", project),
                        "docker", "compose", "-p", project, "up", "-d", "--wait");
            }
            final String url = url(project);
            Docker.await(project, START_TIMEOUT_MS, new Docker.Ready() {
                @Override
                public boolean isReady() {
                    return Docker.httpOk(url + "/_cluster/health", 2000);
                }
            });
            urls.add(url);
        }
        return urls;
    }

    /** @return whether that cluster has a running container */
    public boolean isRunning(String project) {
        Docker.Result result = Docker.attempt(composeDir, Docker.env("CLUSTER_NAME", project),
                "docker", "compose", "-p", project, "ps", "--status", "running", "-q");
        return result.exitCode == 0 && !result.output.trim().isEmpty();
    }

    /**
     * @return {@code http://localhost:<published port>} for that cluster
     * @throws IllegalStateException if the cluster publishes nothing
     */
    public String url(String project) {
        String published = Docker.run(composeDir, Docker.env("CLUSTER_NAME", project),
                "docker", "compose", "-p", project, "port", SERVICE, String.valueOf(ES_PORT)).trim();

        // Docker answers with a bind address, "0.0.0.0:32768". The address is
        // not somewhere you can connect to; only the port is useful.
        int colon = published.lastIndexOf(':');
        if (colon < 0) {
            throw new IllegalStateException("no published port for " + project + ": '" + published + "'");
        }
        return "http://localhost:" + published.substring(colon + 1).trim();
    }

    /** @return the name other containers reach this cluster by, for a remote reindex source */
    public static String internalUrl(int cluster) {
        return "http://" + project(cluster) + ":" + ES_PORT;
    }

    private void ensureSharedNetwork() {
        Docker.Result exists = Docker.attempt(composeDir, Docker.env("CLUSTER_NAME", "es-1"),
                "docker", "network", "inspect", SHARED_NETWORK);
        if (exists.exitCode != 0) {
            Docker.run(composeDir, "docker", "network", "create", SHARED_NETWORK);
        }
    }
}
