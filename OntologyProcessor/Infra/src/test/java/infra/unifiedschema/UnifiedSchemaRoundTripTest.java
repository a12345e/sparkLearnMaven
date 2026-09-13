package infra.unifiedschema;

import infra.docker.HadoopStack;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The full life of a unified table: created from {@code unified_schema.sql},
 * filled, searched, and then taken away again.
 *
 * <p>The DDL is not repeated here. The test reads the schema file itself, so a
 * column added or renamed there is exercised on the next run without anyone
 * touching this file - and, more to the point, a schema file that does not
 * parse fails here rather than in whatever job first tries to use it.
 *
 * <p>Not part of {@code mvn test} - it needs docker. Run it with:
 *
 * <pre>{@code
 * mvn -pl Infra test -DskipInfraTests=false -Dtest=UnifiedSchemaRoundTripTest
 * }</pre>
 *
 * <h2>The three things the schema file cannot be used verbatim for</h2>
 *
 * <p><b>The table name.</b> The file names the real table,
 * {@code ontology.unified_basic}. This test writes to
 * {@code ontology.unified_basic_test} so a run on a machine that has the real
 * table cannot drop it at the end.
 *
 * <p><b>The location.</b> The file points at HDFS. The data here goes to local
 * disk instead, for the reason {@code InitializeInfraTest} explains at length:
 * it isolates the metastore, so when this passes, the catalog round trip is
 * proven on its own with HDFS out of the picture. {@code HdfsDirectOpsTest}
 * covers the block IO.
 *
 * <p><b>The trailing semicolon.</b> {@code spark.sql} takes one statement and
 * no terminator.
 *
 * <h2>Why the inserts name every partition</h2>
 *
 * <p>All five partition columns are given values in the {@code PARTITION}
 * clause, so nothing is derived from the selected rows. That keeps the test off
 * {@code hive.exec.dynamic.partition.mode}, which is {@code strict} by default
 * and would reject a fully dynamic insert.
 */
public class UnifiedSchemaRoundTripTest {

    private static final String DATABASE = "ontology";
    private static final String TABLE = "unified_basic_test";
    private static final String QUALIFIED = DATABASE + "." + TABLE;

    /** The schema file this test is about, relative to the Infra module. */
    private static final String SCHEMA_FILE =
            "src/main/java/infra/unifiedschema/unified_schema.sql";

    /** The table the schema file names, and which this test substitutes away. */
    private static final String SCHEMA_FILE_TABLE = "ontology.unified_basic";

    /** Matches hive.version in the parent POM, and ClusterConfig.HIVE_VERSION. */
    private static final String HIVE_METASTORE_VERSION = "3.1.3";

    /** One hour of phone calls. Two rows land here. */
    private static final String PERIOD_ONE = "HOUR_2026010100";

    /** The next hour. One row lands here, so the table ends up with two partitions. */
    private static final String PERIOD_TWO = "HOUR_2026010101";

    private static final String RELATION = "called_HOUR_person_phone";

    private static HadoopStack hadoop;
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
        detail(hadoop.ensureUp() ? "was down; started it and waited for the metastore" : "already up");
        detail("metastore " + hadoop.metastoreUris());

        stage("Starting a local Spark session against the Hive metastore");
        warehouse = new File("target/warehouse").getAbsoluteFile();
        spark = SparkSession.builder()
                .appName("unified-schema-round-trip")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .config("hive.metastore.uris", hadoop.metastoreUris())
                // Spark embeds Hive 2.3.9, so the 3.1.3 client this project
                // declares has to be loaded into an isolated classloader;
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
    public void createsAUnifiedTableFromTheSchemaFileThenFillsSearchesAndDropsIt() {
        File data = new File(warehouse, TABLE);
        String location = data.toURI().toString();

        // ---------------------------------------------------------- create

        stage("Hive: clearing anything a previous run left behind");
        spark.sql("CREATE DATABASE IF NOT EXISTS " + DATABASE);
        boolean existed = spark.catalog().tableExists(QUALIFIED);
        spark.sql("DROP TABLE IF EXISTS " + QUALIFIED);
        detail(existed ? "dropped the table" : "no table to drop");
        // External, so DROP leaves the files. Take them too, or this run counts
        // the previous run's rows.
        detail(deleteRecursively(data) ? "removed its files at " + data : "no files to remove");

        stage("Hive: creating " + QUALIFIED + " from " + SCHEMA_FILE);
        String ddl = unifiedSchemaDdl(QUALIFIED, location);
        detail(ddl.replace('\n', ' ').replaceAll(" +", " "));
        spark.sql(ddl);
        assertTrue("table should exist after CREATE", spark.catalog().tableExists(QUALIFIED));

        stage("Hive: checking the table really has the unified shape");
        StructType schema = spark.table(QUALIFIED).schema();
        detail(schema.treeString());
        assertArrayEquals("columns, data then partitions in declaration order",
                new String[] { "related", "relating", "event",
                               "related_group", "relation", "product", "period", "dt" },
                schema.fieldNames());
        StructType event = (StructType) schema.apply("event").dataType();
        assertArrayEquals("fields of the event struct",
                new String[] { "start", "end", "count_lower_bound",
                               "count_upper_bound", "events_set", "attributes" },
                event.fieldNames());
        assertEquals("event.start", DataTypes.TimestampType, event.apply("start").dataType());
        assertEquals("event.count_lower_bound",
                DataTypes.LongType, event.apply("count_lower_bound").dataType());
        assertEquals("event.events_set",
                DataTypes.TimestampType, ((ArrayType) event.apply("events_set").dataType()).elementType());
        StructType related = (StructType) schema.apply("related").dataType();
        assertEquals("related.attributes values",
                DataTypes.StringType, ((MapType) related.apply("attributes").dataType()).valueType());

        // ---------------------------------------------------------- insert

        stage("Hive: inserting two rows into period " + PERIOD_ONE);
        spark.sql(insertInto(PERIOD_ONE, "20260101000203",
                row("alice", "subscriber", "+972-50-1111111", "msisdn",
                        "2026-01-01 00:00:00", "2026-01-01 01:00:00", 2, 5, "voice",
                        "2026-01-01 00:15:00", "2026-01-01 00:45:00")
                + " UNION ALL "
                + row("bob", "subscriber", "+972-50-2222222", "msisdn",
                        "2026-01-01 00:00:00", "2026-01-01 01:00:00", 1, 1, "sms",
                        "2026-01-01 00:30:00")));

        stage("Hive: inserting one row into period " + PERIOD_TWO);
        spark.sql(insertInto(PERIOD_TWO, "20260101010203",
                row("alice", "subscriber", "+972-50-3333333", "msisdn",
                        "2026-01-01 01:00:00", "2026-01-01 02:00:00", 7, 9, "voice",
                        "2026-01-01 01:05:00", "2026-01-01 01:25:00", "2026-01-01 01:55:00")));

        long written = spark.table(QUALIFIED).count();
        detail("rows in the table " + written);
        assertEquals("rows written", 3L, written);

        stage("Hive: the partitions the inserts created");
        List<Row> partitions = spark.sql("SHOW PARTITIONS " + QUALIFIED).collectAsList();
        for (Row partition : partitions) {
            detail(partition.getString(0));
        }
        assertEquals("one partition per period", 2, partitions.size());

        // ---------------------------------------------------------- search

        stage("Hive: selecting everything, to see what is in there");
        for (Row row : spark.sql("SELECT relating.value, related.value, event.count_lower_bound,"
                + " event.attributes['channel'], period FROM " + QUALIFIED
                + " ORDER BY period, related.value").collectAsList()) {
            detail(row.toString());
        }

        stage("Hive: the search - one partition, and a predicate inside each nested column");
        // Partition columns prune whole directories; the struct and map lookups
        // are what prove the nested shape survived the parquet round trip.
        Dataset<Row> found = spark.sql(
                "SELECT relating.value              AS caller,"
              + "       related.value               AS callee,"
              + "       related.attributes['kind']  AS callee_kind,"
              + "       event.`start`               AS started,"
              + "       event.`end`                 AS ended,"
              + "       event.count_lower_bound     AS at_least,"
              + "       event.count_upper_bound     AS at_most,"
              + "       size(event.events_set)      AS events,"
              + "       event.attributes['channel'] AS channel"
              + "  FROM " + QUALIFIED
              + " WHERE related_group = '0'"
              + "   AND relation      = '" + RELATION + "'"
              + "   AND product       = '1'"
              + "   AND period        = '" + PERIOD_ONE + "'"
              + "   AND event.attributes['channel'] = 'voice'"
              + "   AND event.count_lower_bound >= 2");
        List<Row> hits = found.collectAsList();
        for (Row hit : hits) {
            detail(hit.toString());
        }
        assertEquals("only alice's voice call is in that hour above that bound", 1, hits.size());

        Row hit = hits.get(0);
        assertEquals("caller", "alice", hit.getAs("caller"));
        assertEquals("callee", "+972-50-1111111", hit.getAs("callee"));
        assertEquals("callee kind, out of the related attributes map", "msisdn", hit.getAs("callee_kind"));
        assertEquals("event start", "2026-01-01 00:00:00.0", hit.getAs("started").toString());
        assertEquals("event end", "2026-01-01 01:00:00.0", hit.getAs("ended").toString());
        assertEquals("lower bound", 2L, ((Number) hit.getAs("at_least")).longValue());
        assertEquals("upper bound", 5L, ((Number) hit.getAs("at_most")).longValue());
        assertEquals("timestamps in the event set", 2, ((Number) hit.getAs("events")).intValue());
        assertEquals("channel, out of the event attributes map", "voice", hit.getAs("channel"));

        stage("Hive: the same search one hour later, to show the partition really narrows it");
        long nextHour = spark.sql("SELECT 1 FROM " + QUALIFIED
                + " WHERE period = '" + PERIOD_TWO + "'"
                + "   AND event.attributes['channel'] = 'voice'").count();
        detail("rows " + nextHour);
        assertEquals("the third row, on its own", 1L, nextHour);

        // ----------------------------------------------------------- drop

        stage("Hive: dropping " + QUALIFIED);
        spark.sql("DROP TABLE " + QUALIFIED);
        assertFalse("table should be gone after DROP", spark.catalog().tableExists(QUALIFIED));
        detail("catalog entry gone");

        // The table is EXTERNAL, so the parquet is still on disk. Removing it
        // is part of "remove it" - and without it the next run starts dirty.
        stage("Removing the files DROP left behind, because the table was external");
        assertTrue("the external files should still be there after DROP", data.isDirectory());
        assertTrue("should have removed the data directory", deleteRecursively(data));
        assertFalse("data directory should be gone", data.exists());
        detail("removed " + data);

        stage("Done: created from the schema file, 3 rows in 2 partitions, searched, and removed");
    }

    /**
     * The single CREATE statement in {@code unified_schema.sql}, retargeted.
     *
     * @param table    replaces the table the file names
     * @param location replaces the HDFS location the file names, as a URI
     */
    private static String unifiedSchemaDdl(String table, String location) {
        String ddl = read(schemaFile());
        if (!ddl.contains(SCHEMA_FILE_TABLE)) {
            throw new IllegalStateException(
                    SCHEMA_FILE + " no longer creates " + SCHEMA_FILE_TABLE
                  + "; this test substitutes that name and cannot any more");
        }
        return ddl.replace(SCHEMA_FILE_TABLE, table)
                  // LOCATION '...' is the last clause, and the only quoted path.
                  .replaceAll("(?is)LOCATION\\s*'[^']*'", "LOCATION '" + location + "'")
                  .trim()
                  // spark.sql takes one statement, without a terminator.
                  .replaceAll(";\\s*$", "");
    }

    /**
     * Surefire runs with the module as the working directory, but a reactor
     * build or an IDE may start a level up, so try both rather than depending
     * on which.
     */
    private static File schemaFile() {
        File inModule = new File(SCHEMA_FILE);
        if (inModule.isFile()) {
            return inModule;
        }
        File fromRoot = new File("Infra", SCHEMA_FILE);
        if (fromRoot.isFile()) {
            return fromRoot;
        }
        throw new IllegalStateException("cannot find " + SCHEMA_FILE
                + " from working directory " + new File(".").getAbsolutePath());
    }

    private static String read(File file) {
        try {
            return new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("reading " + file, e);
        }
    }

    /** An INSERT naming every partition, for rows built by {@link #row}. */
    private static String insertInto(String period, String dt, String selects) {
        return "INSERT INTO TABLE " + QUALIFIED + " PARTITION ("
             + "related_group = '0',"
             + "relation      = '" + RELATION + "',"
             + "product       = '1',"
             + "period        = '" + period + "',"
             + "dt            = '" + dt + "')"
             + " " + selects;
    }

    /**
     * One row as a SELECT of three structs.
     *
     * <p>{@code named_struct} rather than a tuple because the field names have
     * to match the ones in the schema file - Spark checks them on insert, and
     * {@code start} and {@code end} could not be written as bare identifiers
     * here anyway.
     */
    private static String row(String relatingValue, String relatingRole,
                              String relatedValue, String relatedKind,
                              String start, String end,
                              long countLowerBound, long countUpperBound,
                              String channel, String... eventTimes) {
        StringBuilder events = new StringBuilder("array(");
        for (int i = 0; i < eventTimes.length; i++) {
            events.append(i == 0 ? "" : ", ").append("TIMESTAMP '").append(eventTimes[i]).append("'");
        }
        events.append(")");

        return "SELECT named_struct("
             + "  'value', '" + relatedValue + "',"
             + "  'attributes', map('kind', '" + relatedKind + "')) AS related,"
             + " named_struct("
             + "  'value', '" + relatingValue + "',"
             + "  'attributes', map('role', '" + relatingRole + "')) AS relating,"
             + " named_struct("
             + "  'start', TIMESTAMP '" + start + "',"
             + "  'end', TIMESTAMP '" + end + "',"
             + "  'count_lower_bound', CAST(" + countLowerBound + " AS BIGINT),"
             + "  'count_upper_bound', CAST(" + countUpperBound + " AS BIGINT),"
             + "  'events_set', " + events + ","
             + "  'attributes', map('channel', '" + channel + "')) AS event";
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
