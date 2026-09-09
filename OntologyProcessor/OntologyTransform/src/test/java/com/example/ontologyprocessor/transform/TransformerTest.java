package com.example.ontologyprocessor.transform;

import org.junit.Test;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.upper;

/**
 * Specifies the {@link Transformer} contract, and doubles as this module's
 * infrastructure smoke test.
 */
public class TransformerTest extends SparkTestSupport {

    private static final String SCHEMA = "id INT, name STRING";

    private static org.apache.spark.sql.Dataset<org.apache.spark.sql.Row> people() {
        return df(SCHEMA,
                row(1, "ada"),
                row(2, "grace"),
                row(3, "alan"));
    }

    @Test
    public void identityLeavesTheDatasetUnchanged() {
        assertData(people(), Transformer.identity().transform(people()));
    }

    @Test
    public void transformsRows() {
        Transformer toUpper = input -> input.withColumn("name", upper(col("name")));

        assertData(df(SCHEMA,
                row(1, "ADA"),
                row(2, "GRACE"),
                row(3, "ALAN")), toUpper.transform(people()));
    }

    @Test
    public void andThenRunsStepsInOrder() {
        Transformer dropGrace = input -> input.filter(col("id").notEqual(2));
        Transformer toUpper = input -> input.withColumn("name", upper(col("name")));

        assertData(df(SCHEMA,
                row(1, "ADA"),
                row(3, "ALAN")), dropGrace.andThen(toUpper).transform(people()));
    }

    @Test
    public void andThenIsAssociative() {
        Transformer a = input -> input.filter(col("id").notEqual(2));
        Transformer b = input -> input.withColumn("name", upper(col("name")));
        Transformer c = input -> input.withColumn("id", col("id").multiply(10));

        assertData(a.andThen(b).andThen(c).transform(people()),
                a.andThen(b.andThen(c)).transform(people()));
    }

    @Test
    public void composingDoesNotTouchTheInput() {
        Transformer toUpper = input -> input.withColumn("name", upper(col("name")));

        Transformer unused = Transformer.identity().andThen(toUpper);
        org.junit.Assert.assertNotNull(unused);

        // Datasets are immutable: composing over `people()` cannot have changed it.
        assertData(df(SCHEMA,
                row(1, "ada"),
                row(2, "grace"),
                row(3, "alan")), people());
    }

    @Test(expected = IllegalArgumentException.class)
    public void andThenRejectsNull() {
        Transformer.identity().andThen(null);
    }
}
