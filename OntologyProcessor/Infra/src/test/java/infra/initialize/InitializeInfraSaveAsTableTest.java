package infra.initialize;

import infra.docker.ElasticStack;
import infra.docker.HadoopStack;
import infra.elastic.ElasticRest;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The same round trip as {@link InitializeInfraTest}, with the Hive table made
 * through the {@code DataFrameWriter} API instead of SQL.
 *
 * <p>Three things differ, and they are the reason this exists as its own test
 * rather than as an edit to the other one:
 *
 * <ul>
 *   <li><b>One call instead of two.</b> {@code saveAsTable} registers the table
 *       and writes the rows together, so there is no {@code CREATE} followed by
 *       an {@code INSERT}, and no schema written out by hand - it comes from
 *       the dataset.</li>
 *   <li><b>No hand-built SQL.</b> The other test interpolates a filesystem path
 *       into a SQL string, which is the kind of thing that breaks the first
 *       time a path contains a quote or a backslash.</li>
 *   <li><b>No manual file cleanup.</b> {@link SaveMode#Overwrite} replaces what
 *       is at the location, so this test does not need the
 *       {@code deleteRecursively} the other one needs to stay repeatable.</li>
 * </ul>
 *
 * <p>What does <em>not</em> differ is the table type. {@code option("path", ...)}
 * marks the table EXTERNAL exactly as SQL's {@code LOCATION} does - Spark
 * decides from the presence of a location, not from an {@code EXTERNAL}
 * keyword, which is not even available on this path. Both tests therefore
 * produce an {@code EXTERNAL_TABLE} in the metastore.
 *
 * <p>Uses {@code tableB} and {@code indexb} so it cannot collide with
 * {@link InitializeInfraTest}.
 *
 * <pre>{@code
 * mvn -pl Infra test -DskipInfraTests=false
 * }</pre>
 */
public class InitializeInfraSaveAsTableTest {

    private static final String TABLE = "tableB";
    private static final String INDEX = "indexb";
    private static final String COLUMN = "columnA";

    /** Matches hive.version in the parent POM, and ClusterConfig.HIVE_VERSION. */
    private static final String HIVE_METASTORE_VERSION = "3.1.3";

    private static final String INDEX_BODY =
            "{"
          + "  \"settings\": { \"number_of_shards\": 1, \"number_of_replicas\": 0 },"
          + "  \"mappings\": { \"properties\": { \"" + COLUMN + "\": { \"type\": \"keyword\" } } }"
          + "}";

    private static HadoopStack hadoop;
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
        stage("Hadoop/Hive docker: checking whether it is already up");
        hadoop = HadoopStack.packaged();
        detail(hadoop.ensureUp() ? "was down; started it" : "already up");
        detail("metastore " + hadoop.metastoreUris());

        stage("Elasticsearch docker: checking whether one cluster is already up");
        ElasticStack elastic = ElasticStack.packaged();
        boolean wasRunning = elastic.isRunning(ElasticStack.project(1));
        List<String> urls = elastic.ensureUp(1);
        detail(wasRunning ? "already up" : "was down; started one cluster");
        detail("cluster url " + urls.get(0));
        es = new ElasticRest(urls.get(0));

        stage("Both stacks ready: starting a local Spark session against the Hive metastore");
        warehouse = new File("target/warehouse").getAbsoluteFile();
        spark = SparkSession.builder()
                .appName("infra-initialize-saveastable")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .config("hive.metastore.uris", hadoop.metastoreUris())
                .config("spark.sql.hive.metastore.version", HIVE_METASTORE_VERSION)
                .config("spark.sql.hive.metastore.jars", "maven")
                .config("spark.sql.warehouse.dir", warehouse.toURI().toString())
                .enableHiveSupport()
                .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
        detail("spark " + spark.version() + ", master local[2], hive support on");
    }

    @AfterClass
    public static void stopSpark() {
        if (spark != null) {
            spark.stop();
            spark = null;
        }
    }

    private Dataset<Row> people() {
        StructType schema = StructType.fromDDL(COLUMN + " STRING");
        return spark.createDataFrame(Arrays.asList(
                RowFactory.create("ada"),
                RowFactory.create("grace"),
                RowFactory.create("alan"),
                RowFactory.create("hopper")), schema);
    }

    @Test
    public void createsTheHiveTableWithSaveAsTableThenCopiesItIntoElasticsearch() {
        String location = new File(warehouse, TABLE.toLowerCase()).toURI().toString();

        stage("Hive: dropping table " + TABLE + " if it is there");
        boolean existed = spark.catalog().tableExists(TABLE);
        spark.sql("DROP TABLE IF EXISTS " + TABLE);
        detail(existed ? "dropped it" : "was not there, nothing to drop");

        stage("Hive: creating " + TABLE + " (" + COLUMN + " STRING) and writing rows, in one call");
        // option("path", ...) is what makes this EXTERNAL - the same rule SQL's
        // LOCATION goes through. Overwrite replaces whatever the last run left
        // at that path, which is why no directory has to be cleaned by hand.
        people().write()
                .format("parquet")
                .option("path", location)
                .mode(SaveMode.Overwrite)
                .saveAsTable(TABLE);
        assertTrue("table should exist after saveAsTable", spark.catalog().tableExists(TABLE));
        long written = spark.table(TABLE).count();
        detail("created and wrote " + written + " rows, location " + location);
        assertEquals("rows in the table", 4L, written);

        stage("Elasticsearch: deleting index " + INDEX + " if it is there");
        boolean indexExisted = es.indexExists(INDEX);
        es.deleteIndex(INDEX);
        detail(indexExisted ? "deleted it" : "was not there, nothing to delete");

        stage("Elasticsearch: creating index " + INDEX + " (one shard, " + COLUMN + " as keyword)");
        es.createIndex(INDEX, INDEX_BODY);
        assertTrue("index should exist after create", es.indexExists(INDEX));
        detail("mapping " + es.mapping(INDEX));

        stage("Elasticsearch: writing " + TABLE + " into " + INDEX + " with Spark");
        URI cluster = URI.create(es.baseUrl());
        spark.table(TABLE).write()
                .format("es")
                .option("es.nodes", cluster.getHost())
                .option("es.port", String.valueOf(cluster.getPort()))
                .option("es.nodes.wan.only", "true")
                .option("es.index.auto.create", "false")
                .mode(SaveMode.Append)
                .save(INDEX);
        es.refresh(INDEX);
        detail("wrote via es.nodes=" + cluster.getHost() + ":" + cluster.getPort());

        stage("Reading back the Hive table " + TABLE);
        Dataset<Row> table = spark.table(TABLE);
        for (Row row : table.collectAsList()) {
            detail(row.toString());
        }
        assertEquals("rows read back from the table", 4L, table.count());

        stage("Reading back the Elasticsearch index " + INDEX);
        long indexed = es.count(INDEX);
        detail("count " + indexed);
        assertEquals("documents in the index", 4L, indexed);

        stage("Done: " + written + " rows in " + TABLE + ", " + indexed + " documents in " + INDEX);
    }
}
