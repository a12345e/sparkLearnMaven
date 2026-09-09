package com.example.ontologyprocessor.transform;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.io.Serializable;

/**
 * The transform stage: one step of dataset-to-dataset work.
 *
 * <p>A {@code Transformer} is a pure, lazy transformation — it returns a new
 * {@code Dataset} without triggering a Spark action. Keeping steps in this shape
 * makes them composable via {@link #andThen} and testable in isolation against
 * small in-memory fixtures.
 *
 * <p>Implementations must be {@link Serializable}: anything captured inside a
 * lambda or UDF is shipped to the executors.
 *
 * <pre>{@code
 * Transformer pipeline = dropBlankNames.andThen(normalizeName);
 * Dataset<Row> out = pipeline.transform(input);
 * }</pre>
 *
 * <p>The domain model in {@link com.example.ontologyprocessor.transform.model}
 * is built from these.
 */
public interface Transformer extends Serializable {

    /**
     * Transforms the input dataset.
     *
     * @param input the dataset to transform, never {@code null}
     * @return the transformed dataset
     */
    Dataset<Row> transform(Dataset<Row> input);

    /**
     * Returns a transformer that runs {@code this} and feeds the result to
     * {@code next}.
     *
     * @param next the step to run second, must not be {@code null}
     */
    default Transformer andThen(Transformer next) {
        if (next == null) {
            throw new IllegalArgumentException("next transformer must not be null");
        }
        return input -> next.transform(this.transform(input));
    }

    /** A transformer that returns its input unchanged. */
    static Transformer identity() {
        return input -> input;
    }
}
