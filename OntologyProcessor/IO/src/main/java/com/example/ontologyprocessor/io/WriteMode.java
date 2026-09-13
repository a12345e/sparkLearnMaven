package com.example.ontologyprocessor.io;

import org.apache.spark.sql.SaveMode;

/**
 * What a write does to data already at the target.
 *
 * <p>The four cases Spark's {@link SaveMode} names, restated here so a caller
 * asks for a behaviour rather than for a Spark enum. A backend that is not
 * Spark-native still has to honour all four - an Elasticsearch store
 * implements {@link #OVERWRITE} by recreating the index, not by handing the
 * value to a {@code DataFrameWriter} - and {@link #saveMode()} is there for the
 * backends that can simply pass it through.
 *
 * <p>"Already holds data" below means <em>rows</em>, not merely an existing
 * table. In this contract a table is created through {@link DdlOperations} and
 * written to afterwards, so it normally exists before the first write; reading
 * existence as the condition would leave {@link #ERROR_IF_EXISTS} and
 * {@link #IGNORE} with nothing to say. A Spark-native backend therefore cannot
 * simply hand {@link #saveMode()} to a writer for those two - it has to ask
 * whether the table is empty first.
 */
public enum WriteMode {

    /** Add rows, leaving anything already there. */
    APPEND(SaveMode.Append),

    /** Replace everything at the target with these rows. */
    OVERWRITE(SaveMode.Overwrite),

    /** Fail if the target already holds data. */
    ERROR_IF_EXISTS(SaveMode.ErrorIfExists),

    /** Do nothing if the target already holds data. */
    IGNORE(SaveMode.Ignore);

    private final SaveMode saveMode;

    WriteMode(SaveMode saveMode) {
        this.saveMode = saveMode;
    }

    /** @return the equivalent Spark mode, for a backend that writes through Spark */
    public SaveMode saveMode() {
        return saveMode;
    }

    /** @return the mode equivalent to a Spark one */
    public static WriteMode from(SaveMode saveMode) {
        for (WriteMode mode : values()) {
            if (mode.saveMode == saveMode) {
                return mode;
            }
        }
        throw new IllegalArgumentException("no WriteMode for SaveMode." + saveMode);
    }
}
