package com.example.ontologyprocessor.io;

import org.apache.spark.sql.types.StructType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Specifies {@link TableSpec}, and in particular that a partition column has
 * to be a column.
 *
 * <p>That check is the reason the type exists rather than a bare triple of
 * arguments: a mistyped partition column is otherwise found by the backend,
 * mid-DDL, in whatever wording Hive chooses.
 */
public class TableSpecTest {

    private static final String DDL = "id INT, name STRING, dt STRING, country STRING";
    private static final TableRef PEOPLE = TableRef.parse("ontology.people");

    private static TableSpec people() {
        return TableSpec.of(PEOPLE, DDL);
    }

    @Test
    public void aSpecCarriesItsTableAndSchema() {
        TableSpec spec = people();

        assertEquals(PEOPLE, spec.table());
        assertEquals(StructType.fromDDL(DDL), spec.schema());
    }

    @Test
    public void aSpecIsUnpartitionedWithNoOptionsByDefault() {
        TableSpec spec = people();

        assertFalse(spec.isPartitioned());
        assertEquals(Collections.emptyList(), spec.partitionColumns());
        assertEquals(Options.none(), spec.options());
    }

    @Test
    public void aSchemaCanBeGivenAsAStructTypeOrAsDdl() {
        assertEquals(TableSpec.of(PEOPLE, DDL), TableSpec.of(PEOPLE, StructType.fromDDL(DDL)));
    }

    @Test
    public void partitionColumnsAreKeptInTheOrderGiven() {
        TableSpec spec = people().partitionedBy("dt", "country");

        assertTrue(spec.isPartitioned());
        assertEquals(Arrays.asList("dt", "country"), spec.partitionColumns());
    }

    @Test
    public void aPartitionColumnMustBeInTheSchema() {
        try {
            people().partitionedBy("dt", "regoin");
            fail("expected a column that is not in the schema to be rejected");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("regoin"));
            assertTrue("names the table", expected.getMessage().contains("ontology.people"));
        }
    }

    /** Spark resolves columns case-insensitively by default, so this does too. */
    @Test
    public void aPartitionColumnMatchesCaseInsensitivelyAndIsStoredAsTheSchemaSpellsIt() {
        TableSpec spec = people().partitionedBy("DT");

        assertEquals("stored under the schema's spelling", Arrays.asList("dt"), spec.partitionColumns());
    }

    @Test
    public void aColumnCannotBeAPartitionTwice() {
        try {
            people().partitionedBy("dt", "DT");
            fail("expected a duplicate partition column to be rejected");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("duplicate"));
        }
    }

    @Test
    public void partitionedByReplacesRatherThanAdds() {
        TableSpec spec = people().partitionedBy("dt", "country").partitionedBy("country");

        assertEquals(Arrays.asList("country"), spec.partitionColumns());
    }

    @Test
    public void partitioningCanBeCleared() {
        TableSpec spec = people().partitionedBy("dt").partitionedBy();

        assertFalse(spec.isPartitioned());
    }

    @Test
    public void aBlankPartitionColumnIsRejected() {
        try {
            people().partitionedBy("  ");
            fail("expected a blank partition column to be rejected");
        } catch (IllegalArgumentException expected) {
            // The point of the test.
        }
    }

    @Test
    public void optionsCanBeSetWholesaleOrOneAtATime() {
        assertEquals(Options.of("format", "parquet"),
                people().withOptions(Options.of("format", "parquet")).options());
        assertEquals(Options.of("format", "parquet", "compression", "snappy"),
                people().withOption("format", "parquet").withOption("compression", "snappy").options());
    }

    @Test
    public void everyChangeLeavesTheOriginalAlone() {
        TableSpec original = people();

        original.partitionedBy("dt");
        original.withOption("format", "parquet");

        assertFalse("original untouched", original.isPartitioned());
        assertTrue("original untouched", original.options().isEmpty());
    }

    @Test
    public void partitioningSurvivesAnOptionChangeAndTheOtherWayRound() {
        TableSpec spec = people().partitionedBy("dt").withOption("format", "parquet");

        assertEquals(Arrays.asList("dt"), spec.partitionColumns());
        assertEquals("parquet", spec.options().get("format"));

        TableSpec other = people().withOption("format", "parquet").partitionedBy("dt");
        assertEquals("order of construction does not matter", spec, other);
    }

    @Test
    public void thePartitionListCannotBeMutated() {
        try {
            people().partitionedBy("dt").partitionColumns().add("country");
            fail("expected partitionColumns() to be unmodifiable");
        } catch (UnsupportedOperationException expected) {
            // The point of the test.
        }
    }

    @Test
    public void aTableMustHaveAtLeastOneColumn() {
        try {
            TableSpec.of(PEOPLE, new StructType());
            fail("expected an empty schema to be rejected");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("ontology.people"));
        }
    }

    @Test
    public void aBlankSchemaDdlIsRejected() {
        try {
            TableSpec.of(PEOPLE, "   ");
            fail("expected blank DDL to be rejected");
        } catch (IllegalArgumentException expected) {
            // The point of the test.
        }
    }

    @Test
    public void specsDescribingTheSameTableAreEqual() {
        TableSpec first = people().partitionedBy("dt").withOption("format", "parquet");
        TableSpec second = TableSpec.of(TableRef.of("ontology", "people"), StructType.fromDDL(DDL))
                .partitionedBy("dt")
                .withOptions(Options.of("format", "parquet"));

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertFalse(first.equals(people()));
    }

    @Test
    public void toStringSaysWhatWouldBeCreated() {
        String described = people().partitionedBy("dt").toString();

        assertTrue(described, described.contains("ontology.people"));
        assertTrue(described, described.contains("partitioned by [dt]"));
    }
}
