package com.example.ontologyprocessor.hadood;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Spark test harness for this module: a shared session, DDL-driven fixtures and
 * dataset assertions.
 *
 * <p>Each ETL module carries its own copy so the three stay independent — no
 * module needs another to compile or test. Keep the copies in step when you
 * change one.
 *
 * <p>The session is created once per test JVM and torn down by a shutdown hook;
 * tests must never stop it. Its warehouse and metastore are redirected into a
 * unique temp directory, so runs leave no {@code spark-warehouse/},
 * {@code metastore_db/} or {@code derby.log} behind and cannot collide.
 */
public abstract class SparkTestSupport {

    private static volatile SparkSession session;

    /** @return the session shared by every test in this JVM */
    protected static SparkSession spark() {
        SparkSession local = session;
        if (local == null) {
            synchronized (SparkTestSupport.class) {
                local = session;
                if (local == null) {
                    local = create();
                    session = local;
                }
            }
        }
        return local;
    }

    private static SparkSession create() {
        File scratch = new File(System.getProperty("java.io.tmpdir"),
                "ontology-processor-tests-" + UUID.randomUUID());

        SparkSession created = SparkSession.builder()
                .appName("ontology-processor-tests")
                .master("local[2]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.default.parallelism", "2")
                .config("spark.sql.warehouse.dir", new File(scratch, "warehouse").getAbsolutePath())
                .getOrCreate();

        created.sparkContext().setLogLevel("WARN");

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                SparkSession local = session;
                session = null;
                if (local != null) {
                    try {
                        local.stop();
                    } catch (RuntimeException ignored) {
                        // The JVM is going away regardless.
                    }
                }
            }
        }, "shared-spark-session-shutdown"));

        return created;
    }

    /** @return a fresh temp directory, for tests that need a real path */
    protected static File tempDir(String prefix) {
        File dir = new File(System.getProperty("java.io.tmpdir"), prefix + "-" + UUID.randomUUID());
        if (!dir.mkdirs()) {
            throw new IllegalStateException("could not create temp dir " + dir);
        }
        return dir;
    }

    // ---------------------------------------------------------------- fixtures

    /**
     * Builds a dataset from a DDL schema and literal rows.
     *
     * <p>Declaring the schema explicitly keeps types unambiguous, which matters
     * when a step depends on a column being {@code INT} rather than
     * {@code BIGINT}.
     *
     * @param schemaDdl e.g. {@code "id INT, name STRING"}
     */
    protected static Dataset<Row> df(String schemaDdl, Object[]... rows) {
        StructType schema = StructType.fromDDL(schemaDdl);
        List<Row> converted = new ArrayList<Row>(rows.length);
        for (int i = 0; i < rows.length; i++) {
            if (rows[i].length != schema.length()) {
                throw new IllegalArgumentException("row " + i + " has " + rows[i].length
                        + " value(s) but schema '" + schemaDdl + "' declares " + schema.length());
            }
            converted.add(RowFactory.create(rows[i]));
        }
        return spark().createDataFrame(converted, schema);
    }

    /** Builds an empty dataset with the given DDL schema. */
    protected static Dataset<Row> emptyDf(String schemaDdl) {
        return spark().createDataFrame(Collections.<Row>emptyList(), StructType.fromDDL(schemaDdl));
    }

    /** Wraps literal values as one row; {@code null} is a valid value. */
    protected static Object[] row(Object... values) {
        return values;
    }

    // -------------------------------------------------------------- assertions

    protected static void assertRowCount(long expected, Dataset<Row> actual) {
        assertEquals("row count", expected, actual.count());
    }

    /** Asserts field names, types and nullability against a DDL string. */
    protected static void assertSchema(String expectedDdl, Dataset<Row> actual) {
        assertEquals("schema", describe(StructType.fromDDL(expectedDdl)), describe(actual.schema()));
    }

    protected static void assertColumns(Dataset<Row> actual, String... expected) {
        assertEquals("columns", Arrays.asList(expected), Arrays.asList(actual.columns()));
    }

    /**
     * Asserts both datasets hold the same rows, <em>ignoring order</em>.
     *
     * <p>The default choice: Spark promises no ordering unless asked, so an
     * order-sensitive assertion is flaky by construction.
     */
    protected static void assertData(Dataset<Row> expected, Dataset<Row> actual) {
        List<String> expectedRows = renderSorted(expected);
        List<String> actualRows = renderSorted(actual);
        if (!expectedRows.equals(actualRows)) {
            fail(diff("datasets differ (order-insensitive)", expectedRows, actualRows));
        }
    }

    /** Asserts the same rows in the same order; use only after an explicit {@code orderBy}. */
    protected static void assertDataInOrder(Dataset<Row> expected, Dataset<Row> actual) {
        List<String> expectedRows = render(expected);
        List<String> actualRows = render(actual);
        if (!expectedRows.equals(actualRows)) {
            fail(diff("datasets differ (order-sensitive)", expectedRows, actualRows));
        }
    }

    /** Asserts one column's values, ignoring order. */
    protected static void assertColumnValues(String column, Dataset<Row> actual, Object... expected) {
        List<String> expectedValues = new ArrayList<String>(expected.length);
        for (Object value : expected) {
            expectedValues.add(String.valueOf(value));
        }
        Collections.sort(expectedValues);

        List<String> actualValues = new ArrayList<String>();
        for (Row row : actual.select(new Column(column)).collectAsList()) {
            actualValues.add(String.valueOf(row.get(0)));
        }
        Collections.sort(actualValues);

        if (!expectedValues.equals(actualValues)) {
            fail(diff("column '" + column + "' differs", expectedValues, actualValues));
        }
    }

    // ----------------------------------------------------------------- helpers

    private static List<String> render(Dataset<Row> dataset) {
        List<Row> rows = dataset.collectAsList();
        List<String> rendered = new ArrayList<String>(rows.size());
        for (Row row : rows) {
            rendered.add(row.toString());
        }
        return rendered;
    }

    private static List<String> renderSorted(Dataset<Row> dataset) {
        List<String> rendered = render(dataset);
        Collections.sort(rendered);
        return rendered;
    }

    private static List<String> describe(StructType schema) {
        List<String> described = new ArrayList<String>(schema.length());
        for (StructField field : schema.fields()) {
            described.add(field.name() + " " + field.dataType().simpleString()
                    + (field.nullable() ? " NULL" : " NOT NULL"));
        }
        return described;
    }

    private static String diff(String headline, List<String> expected, List<String> actual) {
        StringBuilder sb = new StringBuilder(headline).append('\n');
        sb.append("  expected (").append(expected.size()).append(" row(s)):\n");
        appendAll(sb, expected);
        sb.append("  actual   (").append(actual.size()).append(" row(s)):\n");
        appendAll(sb, actual);
        return sb.toString();
    }

    private static void appendAll(StringBuilder sb, List<String> rows) {
        if (rows.isEmpty()) {
            sb.append("    <empty>\n");
            return;
        }
        for (String row : rows) {
            sb.append("    ").append(row).append('\n');
        }
    }
}
