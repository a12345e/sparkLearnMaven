package com.example.ontologyprocessor.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.junit.Test;

import java.io.File;

/**
 * Specifies the {@link Loader} contract, and doubles as this module's
 * infrastructure smoke test.
 *
 * <p>A loader is the pipeline's action, so it is specified by writing for real
 * and reading the result back — asserting on what landed in the sink, not on
 * what the loader was asked to do.
 */
public class LoaderTest extends SparkTestSupport {

    private static final String SCHEMA = "id INT, name STRING";

    private Dataset<Row> people() {
        return df(SCHEMA,
                row(1, "ada"),
                row(2, "grace"),
                row(3, "alan"));
    }

    private File sink(String prefix) {
        return new File(tempDir(prefix), "out");
    }

    @Test
    public void writesEveryRowToTheSink() {
        File target = sink("load-basic");
        Loader parquet = dataset -> dataset.write().parquet(target.getAbsolutePath());

        parquet.load(people());

        assertData(people(), spark().read().parquet(target.getAbsolutePath()));
    }

    @Test
    public void preservesTheSchemaThroughARoundTrip() {
        File target = sink("load-schema");
        Loader parquet = dataset -> dataset.write().parquet(target.getAbsolutePath());

        parquet.load(people());

        assertSchema(SCHEMA, spark().read().parquet(target.getAbsolutePath()));
    }

    @Test
    public void overwriteReplacesRatherThanAppends() {
        File target = sink("load-overwrite");
        Loader parquet = dataset ->
                dataset.write().mode(SaveMode.Overwrite).parquet(target.getAbsolutePath());

        parquet.load(people());
        parquet.load(df(SCHEMA, row(9, "hopper")));

        assertData(df(SCHEMA, row(9, "hopper")),
                spark().read().parquet(target.getAbsolutePath()));
    }

    @Test
    public void writesAnEmptyDatasetWithoutFailing() {
        File target = sink("load-empty");
        Loader parquet = dataset -> dataset.write().parquet(target.getAbsolutePath());

        parquet.load(emptyDf(SCHEMA));

        Dataset<Row> written = spark().read().parquet(target.getAbsolutePath());
        assertRowCount(0, written);
        assertSchema(SCHEMA, written);
    }
}
