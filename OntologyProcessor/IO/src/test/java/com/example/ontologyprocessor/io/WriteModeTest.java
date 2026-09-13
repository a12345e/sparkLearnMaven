package com.example.ontologyprocessor.io;

import org.apache.spark.sql.SaveMode;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Specifies {@link WriteMode}'s correspondence with Spark's {@link SaveMode}.
 *
 * <p>A backend that writes through Spark passes the mapping straight through,
 * so a wrong pairing here would silently append where a caller asked to
 * overwrite. Cheap to pin down, expensive to discover.
 */
public class WriteModeTest {

    @Test
    public void eachModeMapsToItsSparkEquivalent() {
        assertEquals(SaveMode.Append, WriteMode.APPEND.saveMode());
        assertEquals(SaveMode.Overwrite, WriteMode.OVERWRITE.saveMode());
        assertEquals(SaveMode.ErrorIfExists, WriteMode.ERROR_IF_EXISTS.saveMode());
        assertEquals(SaveMode.Ignore, WriteMode.IGNORE.saveMode());
    }

    @Test
    public void everyModeRoundTripsThroughSpark() {
        for (WriteMode mode : WriteMode.values()) {
            assertNotNull(mode + " has no SaveMode", mode.saveMode());
            assertEquals(mode, WriteMode.from(mode.saveMode()));
        }
    }

    /** Every Spark mode is covered, so a Spark upgrade adding one fails here. */
    @Test
    public void everySparkModeHasAWriteMode() {
        for (SaveMode saveMode : SaveMode.values()) {
            assertNotNull(saveMode + " has no WriteMode", WriteMode.from(saveMode));
        }
    }
}
