package com.example.ontologyprocessor.load;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * The load stage: a sink for rows.
 *
 * <p>A {@code Loader} owns everything sink-specific — path, format, save mode,
 * partitioning — so that nothing upstream needs to know where the data is going.
 *
 * <p>Unlike {@link com.example.ontologyprocessor.extract.Extractor} and
 * {@link com.example.ontologyprocessor.transform.Transformer}, a loader is
 * <em>not</em> lazy: it is the action that ends the pipeline and actually runs
 * the plan built by the earlier stages. That is why it returns nothing.
 *
 * <pre>{@code
 * Loader parquet = dataset -> dataset.write().mode(SaveMode.Overwrite).parquet("/out/people");
 * }</pre>
 *
 * <p>This module deliberately ships no implementations. Loaders are written and
 * specified under test first; see {@code LoaderTest}.
 */
public interface Loader extends Serializable {

    /**
     * Writes the dataset to this stage's sink.
     *
     * <p>Triggers a Spark job.
     *
     * @param dataset the dataset to write, never {@code null}
     */
    void load(Dataset<Row> dataset);
}
