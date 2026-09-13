package infra.datasource;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Specifies {@link OntologyDataSource#fromJson(String)}.
 *
 * <p>These definitions are hand-written JSON, so the behaviour worth pinning
 * down is what happens to imperfect input: a missing field is tolerated, but a
 * misspelled one is not. Silently ignoring a typo in a data source definition
 * would mean a job quietly running against the wrong table.
 *
 * <p>Pure unit tests - no docker, no Spark.
 */
public class OntologyDataSourceJsonTest {

    private static final String FULL =
            "{"
            + "\"key\": {\"env\": \"test\", \"id\": 7, \"name\": \"orders\","
            + "          \"product\": \"retail\", \"mission\": \"daily\"},"
            + "\"site\": \"tel-aviv\","
            + "\"extract\": {\"tableName\": \"raw.orders\", \"technology\": \"HIVE\","
            + "              \"sql\": \"select * from raw.orders\"},"
            + "\"transform\": \"normalize_orders\","
            + "\"minimal_dt\": \"2024-01-01\","
            + "\"maximal_dt\": \"2024-12-31\","
            + "\"status\": \"ACTIVE\""
            + "}";

    @Test
    public void readsEveryFieldOfAFullDefinition() {
        OntologyDataSource source = OntologyDataSource.fromJson(FULL);

        assertEquals("tel-aviv", source.getSite());
        assertEquals("normalize_orders", source.getTransform());
        assertEquals("2024-01-01", source.getMinimal_dt());
        assertEquals("2024-12-31", source.getMaximal_dt());
        assertEquals(OntologyDataSourceStatus.ACTIVE, source.getStatus());
    }

    @Test
    public void readsTheNestedKey() {
        OntologyDataSourceKey key = OntologyDataSource.fromJson(FULL).getKey();

        assertEquals("test", key.getEnv());
        assertEquals(7, key.getId());
        assertEquals("orders", key.getName());
        assertEquals("retail", key.getProduct());
        assertEquals("daily", key.getMission());
        assertEquals("7_orders_retail_daily", key.getDataSourceName());
    }

    @Test
    public void readsTheNestedExtract() {
        OntologyDataSourceExtract extract = OntologyDataSource.fromJson(FULL).getExtract();

        assertEquals("raw.orders", extract.getTableName());
        assertEquals("HIVE", extract.getTechnology());
        assertEquals("select * from raw.orders", extract.getSql());
    }

    @Test
    public void aFieldLeftOutComesBackNull() {
        OntologyDataSource source = OntologyDataSource.fromJson("{\"site\": \"tel-aviv\"}");

        assertEquals("tel-aviv", source.getSite());
        assertNull(source.getKey());
        assertNull(source.getExtract());
        assertNull(source.getStatus());
    }

    @Test
    public void everyStatusNameIsAccepted() {
        for (OntologyDataSourceStatus status : OntologyDataSourceStatus.values()) {
            String json = "{\"status\": \"" + status.name() + "\"}";
            assertEquals(status, OntologyDataSource.fromJson(json).getStatus());
        }
    }

    @Test
    public void theHiveConstantIsNotAJsonField() {
        // It is a constant on the class, not part of a definition, so a reader
        // must not be required - or allowed - to supply it.
        OntologyDataSourceExtract extract = OntologyDataSource.fromJson(FULL).getExtract();

        assertEquals("HIVE", extract.HIVE);
    }

    @Test
    public void aMisspelledFieldIsRejected() {
        try {
            OntologyDataSource.fromJson("{\"sight\": \"tel-aviv\"}");
            fail("expected a misspelled field to be rejected");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("sight"));
        }
    }

    @Test
    public void anUnknownStatusIsRejected() {
        try {
            OntologyDataSource.fromJson("{\"status\": \"RETIRED\"}");
            fail("expected an unknown status to be rejected");
        } catch (IllegalArgumentException expected) {
            // the message names the offending value
            assertTrue(expected.getMessage(), expected.getMessage().contains("RETIRED"));
        }
    }

    @Test
    public void textThatIsNotJsonIsRejected() {
        try {
            OntologyDataSource.fromJson("not json at all");
            fail("expected non-json to be rejected");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

    @Test
    public void nullIsRejected() {
        try {
            OntologyDataSource.fromJson(null);
            fail("expected null to be rejected");
        } catch (IllegalArgumentException expected) {
            // expected
        }
    }

}
