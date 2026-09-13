package infra.hdfs;

import infra.docker.Docker;
import infra.docker.HadoopStack;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Files and directories on the real HDFS in docker, created and destroyed from
 * a Spark session running here on the host.
 *
 * <p>This is the piece the two InitializeInfra tests do not do. They point
 * Spark at the Hive metastore in docker but keep the table <em>data</em> on
 * local disk, so nothing they do ever moves an HDFS block. Everything here
 * does: {@link Hdfs} writes through to the DataNode, and one assertion reads
 * the file back with {@code hdfs dfs -cat} <em>inside</em> the container, which
 * is the only way to prove the bytes really left this JVM.
 *
 * <p>Not part of {@code mvn test} - it needs docker. Run it with:
 *
 * <pre>{@code
 * mvn -pl Infra test -DskipInfraTests=false -Dtest=HdfsDirectOpsTest
 * }</pre>
 *
 * <h2>The two settings that make this work</h2>
 *
 * <p>A session that only sets {@code hive.metastore.uris} can create a table
 * and list a directory but cannot read or write a byte, because block IO is a
 * second connection - to the DataNode - and by default a client here dials an
 * address it cannot route to. {@link HadoopStack#sparkHadoopOptions()} carries
 * the fix and explains it; this test just applies it.
 */
public class HdfsDirectOpsTest {

    /** Under /tmp, which the stack provisions 1777, so no permission fiddling. */
    private static final String BASE = "/tmp/infra-hdfs-ops";

    private static HadoopStack hadoop;
    private static SparkSession spark;
    private static Hdfs hdfs;

    private static void stage(String message) {
        System.out.println();
        System.out.println("=== " + message);
    }

    private static void detail(String message) {
        System.out.println("    " + message);
    }

    @BeforeClass
    public static void startInfrastructure() {
        stage("Hadoop/Hive docker: checking whether it is already up");
        hadoop = HadoopStack.packaged();
        if (hadoop.ensureUp()) {
            detail("was down; started it and waited for the namenode and metastore");
        } else {
            detail("already up");
        }
        detail("namenode " + hadoop.hdfsUri());

        stage("DataNode: checking that blocks are reachable from this machine");
        assertTrue(HadoopStack.dataNodeHint(), hadoop.canReachDataNode());
        detail("localhost:" + HadoopStack.DATANODE_DATA_PORT + " is open, so block IO can work");

        stage("Starting a local Spark session with HDFS as its default filesystem");
        SparkSession.Builder builder = SparkSession.builder()
                .appName("infra-hdfs-ops")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2");
        for (Map.Entry<String, String> option : hadoop.sparkHadoopOptions().entrySet()) {
            builder.config(option.getKey(), option.getValue());
            detail(option.getKey() + " = " + option.getValue());
        }
        spark = builder.getOrCreate();
        spark.sparkContext().setLogLevel("WARN");

        hdfs = Hdfs.of(spark);
        detail("spark " + spark.version() + ", filesystem " + hdfs.uri());
        assertTrue("the session should default to HDFS, not local disk",
                hdfs.uri().startsWith("hdfs://"));
    }

    @AfterClass
    public static void stopSpark() {
        if (hdfs != null) {
            hdfs.delete(BASE);
            hdfs = null;
        }
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Test
    public void createsWritesReadsRenamesAndDeletesOnHdfs() {
        String dir = BASE + "/files";
        String file = dir + "/greeting.txt";
        String renamed = dir + "/greeting-archived.txt";
        String contents = "written from a Spark session on the host";

        stage("Clearing " + BASE + " so the run starts from nothing");
        detail(hdfs.delete(BASE) ? "deleted what a previous run left" : "was already clean");
        assertFalse("base should be gone", hdfs.exists(BASE));

        // ------------------------------------------------------- directories

        stage("mkdirs " + dir);
        assertTrue("should have been created", hdfs.mkdirs(dir));
        assertTrue("should exist now", hdfs.exists(dir));
        assertTrue("should be a directory", hdfs.isDirectory(dir));
        detail("created, and its parent " + BASE + " came with it");

        stage("mkdirs " + dir + " again");
        assertFalse("a second mkdirs should report nothing to do", hdfs.mkdirs(dir));
        detail("reported false, as it was already there");

        // ------------------------------------------------------------- files

        stage("Writing " + file);
        hdfs.write(file, contents);
        assertTrue("should exist after write", hdfs.exists(file));
        assertEquals("bytes on disk", contents.getBytes(StandardCharsets.UTF_8).length, hdfs.size(file));
        detail("wrote " + hdfs.size(file) + " bytes");

        stage("Reading it back through the same client");
        assertEquals("what came back", contents, hdfs.read(file));
        detail(hdfs.read(file));

        stage("Reading it back from inside the container, to prove it is really in HDFS");
        // If the write had quietly gone to local disk, or to a Spark-side
        // buffer, this is where it would show: a different process, on a
        // different machine, reading through the NameNode.
        String fromContainer = catInContainer(file);
        assertEquals("what the container sees", contents, fromContainer);
        detail("hdfs dfs -cat in the namenode container returned: " + fromContainer);

        stage("Listing " + dir);
        List<String> listing = hdfs.list(dir);
        for (String entry : listing) {
            detail(entry);
        }
        assertEquals("one file in there", 1, listing.size());
        assertTrue("and it is the one just written", listing.get(0).endsWith("/greeting.txt"));

        // ---------------------------------------------------------- renaming

        stage("Renaming it to " + renamed);
        assertTrue("rename should succeed", hdfs.rename(file, renamed));
        assertFalse("the old name should be gone", hdfs.exists(file));
        assertTrue("the new name should be there", hdfs.exists(renamed));
        assertEquals("and the contents should be untouched", contents, hdfs.read(renamed));
        detail("renamed with no block copy - the contents are byte for byte the same");

        // ---------------------------------------------------------- deleting

        stage("Deleting " + renamed);
        assertTrue("delete should report it removed something", hdfs.delete(renamed));
        assertFalse("and it should be gone", hdfs.exists(renamed));
        detail("deleted");

        stage("Deleting it a second time");
        assertFalse("deleting nothing should report false", hdfs.delete(renamed));
        detail("reported false rather than throwing, so this is safe to call blind");

        stage("Deleting the directory " + dir);
        assertTrue("should have removed it", hdfs.delete(dir));
        assertFalse("should be gone", hdfs.exists(dir));
        detail("removed the directory and everything under it");
    }

    /**
     * The other half: once {@code fs.defaultFS} is HDFS, the ordinary Dataset
     * reader and writer take bare HDFS paths with no scheme and no extra
     * options. This is what a real job does; {@link Hdfs} is for the setup and
     * teardown around it.
     */
    @Test
    public void readsAndWritesADatasetOnAnHdfsPath() {
        String parquet = BASE + "/numbers";

        stage("Clearing " + BASE + " so the run starts from nothing");
        detail(hdfs.delete(BASE) ? "deleted what a previous run left" : "was already clean");

        stage("Writing 1000 rows to " + parquet + " as parquet");
        // No scheme on the path, and no option() naming a filesystem: it lands
        // on HDFS purely because the session says that is the default.
        spark.range(0, 1000).toDF("n").write().parquet(parquet);
        assertTrue("the path should exist", hdfs.exists(parquet));
        assertTrue("and parquet should have made a directory", hdfs.isDirectory(parquet));
        detail("written");

        stage("Looking at what a parquet write actually leaves on HDFS");
        List<String> parts = hdfs.list(parquet);
        for (String entry : parts) {
            detail(entry + "  (" + hdfs.size(entry) + " bytes)");
        }
        assertTrue("a parquet 'file' is a directory of part files plus a marker",
                parts.size() > 1);
        assertTrue("the commit marker should be there",
                parts.get(0).endsWith("/_SUCCESS") || parts.get(parts.size() - 1).endsWith("/_SUCCESS"));

        stage("Reading it back");
        Dataset<Row> numbers = spark.read().parquet(parquet);
        assertEquals("rows read back", 1000L, numbers.count());
        detail("count " + numbers.count());

        stage("Filtering it, so the read really goes to the DataNode");
        long big = numbers.filter("n >= 900").count();
        assertEquals("rows kept", 100L, big);
        detail("n >= 900 kept " + big + " rows");

        stage("Confirming the container sees the same directory");
        String listing = Docker.run(null, "docker", "exec", "namenode",
                "hdfs", "dfs", "-ls", parquet);
        assertTrue("the container should list the part files", listing.contains("part-"));
        detail("hdfs dfs -ls in the namenode container listed " + parts.size() + " entries");

        stage("Deleting " + parquet);
        assertTrue("should have removed it", hdfs.delete(parquet));
        assertFalse("should be gone", hdfs.exists(parquet));
        detail("removed the whole dataset directory");
    }

    /**
     * {@code hdfs dfs -cat} inside the namenode container.
     *
     * <p>Two things get in the way of just reading the output. The hadoop CLI
     * logs to stderr, which {@link Docker#run} folds into stdout, and it does
     * so even for a plain cat - the client announces its SASL check at INFO.
     * So logging is turned down, and only the last non-empty line is taken,
     * which is the file itself.
     */
    private static String catInContainer(String path) {
        String output = Docker.run(null, "docker", "exec",
                "-e", "HADOOP_ROOT_LOGGER=ERROR,console",
                "namenode", "hdfs", "dfs", "-cat", path);
        String[] lines = output.split("\n");
        for (int i = lines.length - 1; i >= 0; i--) {
            String line = lines[i].trim();
            if (!line.isEmpty()) {
                return line;
            }
        }
        throw new AssertionError("nothing came back from cat "
                    + path + ":" + "\n" + output);
    }
}
