package com.example.ontologyprocessor.io;

import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Everything {@link DdlOperations#createTable(TableSpec)} needs: what to call
 * the table, what shape it has, and how the backend should lay it out.
 *
 * <p>The schema is a Spark {@link StructType} rather than anything invented
 * here, because it is already the one description both ends understand - a
 * {@code Dataset}'s schema on the way in, {@code CREATE TABLE} columns or an
 * Elasticsearch mapping on the way out. It also means a table can be declared
 * from the data that will fill it:
 *
 * <pre>{@code
 * TableSpec spec = TableSpec.of(TableRef.of("ontology", "events"), dataset.schema())
 *         .partitionedBy("dt")
 *         .withOptions(Options.of("format", "parquet"));
 * }</pre>
 *
 * <p>Or from a DDL string, which is how the tests write them:
 *
 * <pre>{@code
 * TableSpec.of(TableRef.parse("ontology.people"), "id INT, name STRING")
 * }</pre>
 *
 * <p>Partition columns are checked against the schema when they are set, so a
 * typo fails here rather than as a backend error halfway through a job. They
 * are matched case-insensitively - as Spark resolves columns by default - and
 * stored under the schema's spelling, so a backend can use them verbatim.
 *
 * <p>A backend that has no notion of partitioning is free to ignore them, the
 * way it ignores an {@link Options} key it does not recognise.
 *
 * <p>Immutable; every {@code with}/{@code partitionedBy} returns a new spec.
 */
public final class TableSpec implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TableRef table;
    private final StructType schema;
    private final List<String> partitionColumns;
    private final Options options;

    private TableSpec(TableRef table, StructType schema, List<String> partitionColumns, Options options) {
        this.table = table;
        this.schema = schema;
        this.partitionColumns = Collections.unmodifiableList(partitionColumns);
        this.options = options;
    }

    /** An unpartitioned table with no options. */
    public static TableSpec of(TableRef table, StructType schema) {
        if (table == null) {
            throw new IllegalArgumentException("table must not be null");
        }
        if (schema == null || schema.length() == 0) {
            throw new IllegalArgumentException("schema of " + table + " must declare at least one column");
        }
        return new TableSpec(table, schema, Collections.<String>emptyList(), Options.none());
    }

    /**
     * As {@link #of(TableRef, StructType)}, with the schema given as DDL.
     *
     * @param schemaDdl e.g. {@code "id INT, name STRING"}
     */
    public static TableSpec of(TableRef table, String schemaDdl) {
        if (schemaDdl == null || schemaDdl.trim().isEmpty()) {
            throw new IllegalArgumentException("schemaDdl must not be blank");
        }
        return of(table, StructType.fromDDL(schemaDdl));
    }

    public TableRef table() {
        return table;
    }

    public StructType schema() {
        return schema;
    }

    /** @return the partition columns, in order, or empty */
    public List<String> partitionColumns() {
        return partitionColumns;
    }

    public boolean isPartitioned() {
        return !partitionColumns.isEmpty();
    }

    public Options options() {
        return options;
    }

    /**
     * @return a copy partitioned by these columns, replacing any already set
     * @throws IllegalArgumentException if a column is not in the schema, or is named twice
     */
    public TableSpec partitionedBy(String... columns) {
        if (columns == null || columns.length == 0) {
            return new TableSpec(table, schema, Collections.<String>emptyList(), options);
        }
        List<String> resolved = new ArrayList<String>(columns.length);
        for (String column : columns) {
            String canonical = resolve(column);
            if (resolved.contains(canonical)) {
                throw new IllegalArgumentException("duplicate partition column '" + canonical
                        + "' in " + Arrays.toString(columns));
            }
            resolved.add(canonical);
        }
        return new TableSpec(table, schema, resolved, options);
    }

    /** @return a copy with these options, replacing any already set */
    public TableSpec withOptions(Options options) {
        return new TableSpec(table, schema, new ArrayList<String>(partitionColumns),
                options == null ? Options.none() : options);
    }

    /** @return a copy with one extra option */
    public TableSpec withOption(String key, String value) {
        return withOptions(options.with(key, value));
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TableSpec)) {
            return false;
        }
        TableSpec that = (TableSpec) other;
        return table.equals(that.table)
                && schema.equals(that.schema)
                && partitionColumns.equals(that.partitionColumns)
                && options.equals(that.options);
    }

    @Override
    public int hashCode() {
        int result = table.hashCode();
        result = 31 * result + schema.hashCode();
        result = 31 * result + partitionColumns.hashCode();
        return 31 * result + options.hashCode();
    }

    @Override
    public String toString() {
        return "TableSpec(" + table + " " + schema.simpleString()
                + (isPartitioned() ? " partitioned by " + partitionColumns : "")
                + (options.isEmpty() ? "" : " " + options) + ")";
    }

    /** @return the column's name as the schema spells it */
    private String resolve(String column) {
        if (column == null || column.trim().isEmpty()) {
            throw new IllegalArgumentException("partition column must not be blank");
        }
        String wanted = column.trim();
        for (StructField field : schema.fields()) {
            if (field.name().equalsIgnoreCase(wanted)) {
                return field.name();
            }
        }
        throw new IllegalArgumentException("partition column '" + wanted + "' is not in the schema of "
                + table + " " + schema.simpleString());
    }
}
