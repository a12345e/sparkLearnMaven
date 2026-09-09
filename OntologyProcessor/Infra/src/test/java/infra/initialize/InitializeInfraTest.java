package infra.initialize;

import infra.docker.Docker;
import infra.docker.ElasticStack;
import infra.docker.HadoopStack;
import infra.elastic.ElasticRest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Brings both docker stacks up if they are not already, then drives a full
 * round trip with a locally running Spark: a Hive table created and filled,
 * an Elasticsearch index created and filled from it, and both read back.
 *
 * <p>Every step prints what it is about to do, so a run reads as a narrative
 * rather than as a wall of Spark logging.
 *
 * <p>Not part of `mvn test` - it needs docker. Run it with:
 *
 * <pre>{@code
 * mvn -pl Infra test -DskipInfraTests=false
 * }</pre>
 *
 * <h2>Two things this test has to work around</h2>
 *
 * <p><b>The table data is local, not on HDFS.</b> The DataNode publishes only
 * its UI port and registers under the hostname {@code datanode}, which does
 * not resolve on the host, so a local Spark can talk to the NameNode and the
 * metastore but cannot move blocks. The table is therefore created with an
 * explicit local {@code LOCATION}: the real Hive metastore in docker holds the
 * catalog entry, Spark holds the data. {@link HadoopStack#dataNodeHint()} says
 * what to change to lift that.
 *
 * <p><b>The index is {@code indexa}, not {@code indexA}.</b> Elasticsearch
 * rejects an uppercase character in an index name.
 */
public class InitializeInfraTest {

    private static final String TABLE = "tableA";
    private static final String INDEX = "indexa";
    private static final String COLUMN = "columnA";

    /** Matches hive.version in the parent POM, and ClusterConfig.HIVE_VERSION. */
    private static final String HIVE_METASTORE_VERSION = "3.1.3";

    private static final String INDEX_BODY =
            "{"
          + "  \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 },"
          + "  \"mappings\": { \"properties\": { \"" + COLUMN + "\": { \"type\": \"keyword\" } } }"
          + "}";

    private static HadoopStack hadoop;
    private static ElasticStack elastic;
    private static ElasticRest es;
    private static SparkSession spark;
    private static File warehouse;

    private static void stage(String message) {
        System.out.println();
        System.out.println("=== " + message);
    }

    private static void detail(String message) {
        System.out.println("    " + message);
    }

    @BeforeClass
    public static void startInfrastructure() {
        File root = Docker.projectRoot();

        stage("Hadoop/Hive docker: checking whether it is already up");
        hadoop = HadoopStack.at(root);
        if (hadoop.ensureUp()) {
            detail("was down; started it and waited for the namenode and metastore");
        } else {
            detail("already up");
        }
        detail("namenode  " + hadoop.hdfsUri());
        detail("metastore " + hadoop.metastoreUris());
        if (!hadoop.canReachDataNode()) {
            detail("note: HDFS blocks are NOT reachable from the host, so the table data stays local.");
            detail("      " + HadoopStack.dataNodeHint());
        }

        stage("Elasticsearch docker: checking whether one cluster is already up");
        elastic = ElasticStack.at(root);
        boolean wasRunning = elastic.isRunning(ElasticStack.project(1));
        List<String> urls = elastic.ensureUp(1);
        detail(wasRunning ? "already up" : "was down; started one cluster and waited for it to serve");
        detail("cluster url " + urls.get(0));
        es = new ElasticRest(urls.get(0));

        stage("Both stacks ready: starting a local Spark session against the Hive metastore");
        warehouse = new File(root, "Infra/target/warehouse").getAbsoluteFile();
        spark = SparkSession.builder()
                .appName("infra-initialize")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .config("hive.metastore.uris", hadoop.metastoreUris())
                // The Hive version this project declares. Spark embeds 2.3.9,
                // so 3.1.3 has to be loaded into an isolated classloader;
                // "maven" lets Spark fetch that client itself.
                .config("spark.sql.hive.metastore.version", HIVE_METASTORE_VERSION)
                .config("spark.sql.hive.metastore.jars", "maven")
                .config("spark.sql.warehouse.dir", warehouse.toURI().toString())
                .enableHiveSupport()
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        detail("spark " + spark.version() + ", master local[2], hive support on");
        detail("warehouse " + warehouse);
    }

    @AfterClass
    public static void stopSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    @Test
    public void createsTheHiveTableAndTheElasticIndexAndCopiesOneIntoTheOther() {
        String location = new File(warehouse, TABLE.toLowerCase()).toURI().toString();

        // ------------------------------------------------------------- hive

        stage("Hive: dropping table " + TABLE + " if it is there");
        boolean existed = spark.catalog().tableExists(TABLE);
        spark.sql("DROP TABLE IF EXISTS " + TABLE);
        detail(existed ? "dropped it" : "was not there, nothing to drop");
        // The table is external - it has an explicit LOCATION - so DROP removes
        // the catalog entry and leaves the files behind. Remove them too, or the
        // next run appends to this run's rows instead of starting clean.
        File data = new File(warehouse, TABLE.toLowerCase());
        if (deleteRecursively(data)) {
            detail("removed its files at " + data);
        }

        stage("Hive: creating table " + TABLE + " (" + COLUMN + " STRING)");
        // USING parquet with an explicit LOCATION keeps this a Spark datasource
        // table: the metastore records it, Spark owns the files, and Hive's
        // transactional managed-table default never comes into play.
        spark.sql("CREATE TABLE " + TABLE + " (" + COLUMN + " STRING) "
                + "USING parquet LOCATION '" + location + "'");
        assertTrue("table should exist after CREATE", spark.catalog().tableExists(TABLE));
        detail("created, location " + location);

        stage("Hive: writing rows into " + TABLE);
        spark.sql("INSERT INTO " + TABLE + " VALUES ('ada'), ('grace'), ('alan'), ('hopper')");
        long written = spark.table(TABLE).count();
        detail("wrote " + written + " rows");
        assertEquals("rows in the table", 4L, written);

        // ---------------------------------------------------------- elastic

        stage("Elasticsearch: deleting index " + INDEX + " if it is there");
        boolean indexExisted = es.indexExists(INDEX);
        es.deleteIndex(INDEX);
        detail(indexExisted ? "deleted it" : "was not there, nothing to delete");

        stage("Elasticsearch: creating index " + INDEX + " (one shard, " + COLUMN + " as keyword)");
        es.createIndex(INDEX, INDEX_BODY);
        assertTrue("index should exist after create", es.indexExists(INDEX));
        detail("created");
        detail("mapping " + es.mapping(INDEX));

        stage("Elasticsearch: writing " + TABLE + " into " + INDEX + " with Spark");
        URI cluster = URI.create(es.baseUrl());
        spark.table(TABLE).write()
                .format("es")
                .option("es.nodes", cluster.getHost())
                .option("es.port", String.valueOf(cluster.getPort()))
                // The host reaches the cluster only through its published port;
                // without this the connector would discover the node's own
                // address on the docker network and try to use that.
                .option("es.nodes.wan.only", "true")
                // The index was just created deliberately; do not let a typo
                // silently make a second one.
                .option("es.index.auto.create", "false")
                .mode(SaveMode.Append)
                .save(INDEX);
        es.refresh(INDEX);
        detail("wrote via es.nodes=" + cluster.getHost() + ":" + cluster.getPort());

        // ----------------------------------------------------------- read back

        stage("Reading back the Hive table " + TABLE);
        Dataset<Row> table = spark.table(TABLE);
        for (Row row : table.collectAsList()) {
            detail(row.toString());
        }
        assertEquals("rows read back from the table", 4L, table.count());

        stage("Reading back the Elasticsearch index " + INDEX);
        long indexed = es.count(INDEX);
        detail("count " + indexed);
        detail("search " + es.search(INDEX, 10));
        assertEquals("documents in the index", 4L, indexed);

        stage("Done: " + written + " rows in " + TABLE + ", " + indexed + " documents in " + INDEX);
    }

    /** @return whether anything was there to delete */
    private static boolean deleteRecursively(File file) {
        if (!file.exists()) {
            return false;
        }
        File[] children = file.listFiles();
        if (children != null) {
            for (File child : children) {
                deleteRecursively(child);
            }
        }
        return file.delete();
    }
}
