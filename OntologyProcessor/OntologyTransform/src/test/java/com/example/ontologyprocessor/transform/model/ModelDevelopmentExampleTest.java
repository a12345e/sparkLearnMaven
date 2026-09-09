package com.example.ontologyprocessor.transform.model;

import com.example.ontologyprocessor.transform.SparkTestSupport;
import com.example.ontologyprocessor.transform.Transformer;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.junit.Test;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lower;
import static org.apache.spark.sql.functions.trim;

/**
 * A worked example of the loop the model is developed in — copy this file's
 * shape when specifying a real model element.
 *
 * <p>The candidate step lives <em>inside the test</em>. That is the point: a
 * model element is specified and iterated on here first, and only moves into
 * {@code src/main/java/.../transform/model/} once its behaviour has settled.
 * Nothing outside this module needs to exist for it to run.
 *
 * <p>The four tests below are the questions worth asking of any step: does it
 * do the obvious thing, does it leave the schema alone, what happens to nulls,
 * and what happens to an empty input.
 */
public class ModelDevelopmentExampleTest extends SparkTestSupport {

    private static final String SCHEMA = "id INT, name STRING";

    /**
     * The element under development: normalise {@code name} by trimming
     * surrounding whitespace and lowercasing.
     *
     * <p>A lambda is enough while the behaviour is in flux. Promote it to a
     * named class in this package once it stabilises — the test then keeps
     * passing unchanged, which is the signal the move was clean.
     */
    private static Transformer normalizeName() {
        return input -> input.withColumn("name", lower(trim(col("name"))));
    }

    @Test
    public void trimsAndLowercasesNames() {
        Dataset<Row> input = df(SCHEMA,
                row(1, "  Ada "),
                row(2, "GRACE"),
                row(3, "alan"));

        assertData(df(SCHEMA,
                row(1, "ada"),
                row(2, "grace"),
                row(3, "alan")), normalizeName().transform(input));
    }

    @Test
    public void leavesTheSchemaUnchanged() {
        Dataset<Row> actual = normalizeName().transform(df(SCHEMA, row(1, "Ada")));

        assertColumns(actual, "id", "name");
        // `name` stays a nullable string — the step must not widen or narrow it.
        assertSchema(SCHEMA, actual);
    }

    @Test
    public void propagatesNullsRatherThanInventingValues() {
        Dataset<Row> input = df(SCHEMA,
                row(1, null),
                row(2, "  Bob"));

        assertColumnValues("name", normalizeName().transform(input), null, "bob");
    }

    @Test
    public void handlesEmptyInput() {
        Dataset<Row> actual = normalizeName().transform(emptyDf(SCHEMA));

        assertRowCount(0, actual);
        assertSchema(SCHEMA, actual);
    }
}
