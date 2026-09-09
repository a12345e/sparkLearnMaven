package infra.docker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.JarURLConnection;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * The compose files that describe the two stacks, and getting them onto disk.
 *
 * <p>They live in this module's jar, under the same {@code infra/docker} path
 * as this package, so a project that depends on {@code com.example:Infra}
 * needs nothing checked out to start a stack - one dependency and no files.
 * That is the whole reason they are packaged rather than left in a directory
 * beside the source: the implementation project is a different repository, and
 * a path relative to the working directory would not resolve there.
 *
 * <p>{@code docker compose} cannot read a compose file out of a jar, so they
 * are written to a directory first. That directory is deliberately <em>not</em>
 * a temp dir: the hadoop stack bind-mounts {@code ./conf} and {@code ./scripts}
 * into its containers, so the files have to outlive the JVM that extracted
 * them, and an OS temp sweep while containers were running would break the
 * stack in a way that is very hard to read.
 *
 * <p>Everything is rewritten on each call, so editing a compose file in
 * {@code src/main/resources} and re-running takes effect - Maven copies it to
 * {@code target/classes}, and this copies it on from there.
 *
 * <p>Point {@link #LOCATION_PROPERTY} at a directory to use your own copies
 * instead, in which case nothing is extracted or overwritten.
 */
public final class DockerAssets {

    /** {@code -Dinfra.docker.dir=<path>} to use compose files from disk instead of the jar. */
    public static final String LOCATION_PROPERTY = "infra.docker.dir";

    /** Where the packaged files are on the classpath. */
    private static final String RESOURCE_ROOT = "infra/docker";

    /** The hadoop/hive stack, as a directory name under {@link #directory()}. */
    public static final String HADOOP_STACK = "hadoop313hive313";

    /** The elasticsearch clusters, as a directory name under {@link #directory()}. */
    public static final String ELASTIC_CLUSTERS = "elastic-clusters";

    private DockerAssets() {
    }

    /**
     * @return the directory holding both stacks, extracting them from the jar
     *         unless {@link #LOCATION_PROPERTY} names one already on disk
     */
    public static File directory() {
        String override = System.getProperty(LOCATION_PROPERTY);
        if (override != null && !override.trim().isEmpty()) {
            File chosen = new File(override.trim());
            if (!chosen.isDirectory()) {
                throw new IllegalStateException(LOCATION_PROPERTY + " is not a directory: "
                        + chosen.getAbsolutePath());
            }
            return chosen;
        }
        File target = new File(new File(System.getProperty("user.home"), ".ontology-processor"), "docker");
        extractTo(target);
        return target;
    }

    /** @return the hadoop/hive stack directory, ready for {@code docker compose} */
    public static File hadoopStack() {
        return require(new File(directory(), HADOOP_STACK));
    }

    /** @return the elasticsearch clusters directory, ready for {@code docker compose} */
    public static File elasticClusters() {
        return require(new File(directory(), ELASTIC_CLUSTERS));
    }

    /** Writes the packaged compose files into {@code target}, replacing what is there. */
    public static void extractTo(File target) {
        URL root = loader().getResource(RESOURCE_ROOT);
        if (root == null) {
            throw new IllegalStateException("no " + RESOURCE_ROOT + " on the classpath; "
                    + "the Infra jar is either missing or was built without its resources");
        }
        try {
            if ("file".equals(root.getProtocol())) {
                copyTree(new File(root.toURI()), target);
            } else if ("jar".equals(root.getProtocol())) {
                copyFromJar(root, target);
            } else {
                throw new IllegalStateException("cannot read " + RESOURCE_ROOT + " from " + root);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("could not write the compose files to " + target, e);
        } catch (URISyntaxException e) {
            throw new IllegalStateException("bad resource URL " + root, e);
        }
    }

    // --------------------------------------------------------------- helpers

    /**
     * The resources sit on the same classpath path as this package, so the
     * compiled classes are right beside them. Nothing but the compose files
     * should reach the extraction directory.
     */
    private static boolean isCompiledClass(String name) {
        return name.endsWith(".class");
    }

    /** Exploded classes, which is the case when running out of {@code target/classes}. */
    private static void copyTree(File from, File to) throws IOException {
        if (isCompiledClass(from.getName())) {
            return;
        }
        if (from.isDirectory()) {
            mkdirs(to);
            File[] children = from.listFiles();
            if (children != null) {
                for (File child : children) {
                    copyTree(child, new File(to, child.getName()));
                }
            }
            return;
        }
        mkdirs(to.getParentFile());
        InputStream in = new java.io.FileInputStream(from);
        try {
            write(in, to);
        } finally {
            in.close();
        }
    }

    /** Packaged in a jar, which is the case for any project depending on this one. */
    private static void copyFromJar(URL root, File target) throws IOException {
        JarURLConnection connection = (JarURLConnection) root.openConnection();
        connection.setUseCaches(false);
        JarFile jar = connection.getJarFile();
        String prefix = RESOURCE_ROOT + "/";
        Enumeration<JarEntry> entries = jar.entries();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            String name = entry.getName();
            if (!name.startsWith(prefix) || entry.isDirectory() || isCompiledClass(name)) {
                continue;
            }
            File destination = new File(target, name.substring(prefix.length()));
            mkdirs(destination.getParentFile());
            InputStream in = jar.getInputStream(entry);
            try {
                write(in, destination);
            } finally {
                in.close();
            }
        }
    }

    private static void write(InputStream in, File destination) throws IOException {
        OutputStream out = new FileOutputStream(destination);
        try {
            byte[] chunk = new byte[8192];
            int read;
            while ((read = in.read(chunk)) != -1) {
                out.write(chunk, 0, read);
            }
        } finally {
            out.close();
        }
    }

    private static void mkdirs(File dir) throws IOException {
        if (dir != null && !dir.isDirectory() && !dir.mkdirs() && !dir.isDirectory()) {
            throw new IOException("could not create " + dir.getAbsolutePath());
        }
    }

    private static File require(File stack) {
        if (!new File(stack, "docker-compose.yml").isFile()) {
            throw new IllegalStateException("no docker-compose.yml in " + stack.getAbsolutePath());
        }
        return stack;
    }

    private static ClassLoader loader() {
        ClassLoader contextLoader = Thread.currentThread().getContextClassLoader();
        return contextLoader != null ? contextLoader : DockerAssets.class.getClassLoader();
    }
}
