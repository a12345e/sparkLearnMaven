package infra.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.SparkSession;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Direct file and directory operations on HDFS, driven from a Spark session.
 *
 * <h2>Why this is not a Dataset API</h2>
 *
 * <p>{@code spark.read()} and {@code write()} are for <em>tabular</em> data:
 * they hand back rows, and a path to them is a whole dataset that Spark may
 * spread over many part files. That is the right tool for a parquet directory
 * and the wrong one for "make this directory", "is that file there", "read
 * those twelve lines of JSON", "rename it now the load has finished".
 *
 * <p>Those are filesystem operations, and Hadoop's {@link FileSystem} is the
 * API for them. Spark does not wrap it, but it does carry a fully configured
 * {@link Configuration} - the one built from the session's {@code spark.hadoop.*}
 * settings - and that is all {@link FileSystem} needs. So this is not a second
 * connection to HDFS alongside Spark's; it is the same client configuration,
 * used directly. Anything written here is immediately visible to
 * {@code spark.read()}, and the other way round.
 *
 * <h2>Paths</h2>
 *
 * <p>A path with no scheme resolves against {@code fs.defaultFS}, so
 * {@code /tmp/x} means HDFS when the session was configured with
 * {@link infra.docker.HadoopStack#sparkHadoopOptions()} and a local file
 * otherwise. Spell out {@code hdfs://localhost:8020/tmp/x} when it has to be
 * HDFS regardless of how the session was built.
 *
 * <h2>Lifetime</h2>
 *
 * <p>There is deliberately no close method. {@link FileSystem#get} returns an
 * instance from a JVM-wide cache that Spark itself is using, so closing it
 * would break the session this was built from. It goes when the session goes.
 *
 * <p>Every method turns {@link IOException} into {@link UncheckedIOException}:
 * these are called from tests and from job setup, where an HDFS call failing
 * is not a condition to recover from.
 */
public final class Hdfs {

    private final FileSystem fs;

    private Hdfs(FileSystem fs) {
        this.fs = fs;
    }

    /**
     * @param spark a session whose Hadoop configuration points at the cluster
     * @return operations against that session's default filesystem
     */
    public static Hdfs of(SparkSession spark) {
        Configuration conf = spark.sparkContext().hadoopConfiguration();
        try {
            return new Hdfs(FileSystem.get(conf));
        } catch (IOException e) {
            throw new UncheckedIOException("could not open the filesystem for "
                    + conf.get("fs.defaultFS"), e);
        }
    }

    /** @return where this is pointed, e.g. {@code hdfs://localhost:8020} */
    public String uri() {
        return fs.getUri().toString();
    }

    /** @return the underlying Hadoop handle, for anything not wrapped here */
    public FileSystem raw() {
        return fs;
    }

    public boolean exists(String path) {
        try {
            return fs.exists(new Path(path));
        } catch (IOException e) {
            throw new UncheckedIOException("could not test " + path, e);
        }
    }

    public boolean isDirectory(String path) {
        try {
            return fs.getFileStatus(new Path(path)).isDirectory();
        } catch (IOException e) {
            throw new UncheckedIOException("could not stat " + path, e);
        }
    }

    /**
     * Creates a directory and any missing parents.
     *
     * @return whether it was created, false if it was already there
     */
    public boolean mkdirs(String path) {
        if (exists(path)) {
            return false;
        }
        try {
            return fs.mkdirs(new Path(path));
        } catch (IOException e) {
            throw new UncheckedIOException("could not create " + path, e);
        }
    }

    /**
     * Writes a file, replacing it if it is there. This is the call that needs a
     * reachable DataNode: the bytes go to the DataNode, not to the NameNode.
     */
    public void write(String path, String contents) {
        FSDataOutputStream out = null;
        try {
            out = fs.create(new Path(path), true);
            out.write(contents.getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("could not write " + path, e);
        } finally {
            close(out, path);
        }
    }

    /** @return the whole file as text */
    public String read(String path) {
        FSDataInputStream in = null;
        try {
            in = fs.open(new Path(path));
            BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
            StringBuilder text = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                if (text.length() > 0) {
                    text.append('\n');
                }
                text.append(line);
            }
            return text.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("could not read " + path, e);
        } finally {
            close(in, path);
        }
    }

    /** @return the full paths directly under a directory, sorted */
    public List<String> list(String path) {
        try {
            FileStatus[] children = fs.listStatus(new Path(path));
            List<String> paths = new ArrayList<String>(children.length);
            for (FileStatus child : children) {
                paths.add(child.getPath().toString());
            }
            Collections.sort(paths);
            return paths;
        } catch (IOException e) {
            throw new UncheckedIOException("could not list " + path, e);
        }
    }

    /** @return the size of a file in bytes */
    public long size(String path) {
        try {
            return fs.getFileStatus(new Path(path)).getLen();
        } catch (IOException e) {
            throw new UncheckedIOException("could not stat " + path, e);
        }
    }

    /**
     * A metadata-only move: HDFS renames the entry rather than copying blocks,
     * so this is as cheap for a large directory as for an empty file.
     *
     * @return whether the rename happened
     */
    public boolean rename(String from, String to) {
        try {
            return fs.rename(new Path(from), new Path(to));
        } catch (IOException e) {
            throw new UncheckedIOException("could not rename " + from + " to " + to, e);
        }
    }

    /**
     * Deletes a file, or a directory and everything under it.
     *
     * @return whether anything was deleted, false if it was not there
     */
    public boolean delete(String path) {
        try {
            return fs.delete(new Path(path), true);
        } catch (IOException e) {
            throw new UncheckedIOException("could not delete " + path, e);
        }
    }

    private static void close(Closeable stream, String path) {
        if (stream == null) {
            return;
        }
        try {
            stream.close();
        } catch (IOException e) {
            // A failed close on a write means the file may be incomplete, so
            // this is not something to swallow.
            throw new UncheckedIOException("could not close " + path, e);
        }
    }
}
