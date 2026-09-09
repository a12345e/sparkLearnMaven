package com.example.ontologyprocessor.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * A {@link DataStore} that keeps everything in a map.
 *
 * <p>Test scaffolding, and the reference reading of {@link DataStoreContract}.
 * It exists for two reasons. It proves the contract is satisfiable and
 * self-consistent before any real backend is written, so a failure in a Hive
 * or Elasticsearch store is a fact about that store rather than about the
 * specification. And it gives anything that merely needs <em>a</em> store to
 * test against one that starts instantly and needs no cluster.
 *
 * <p>It is not a fake of any particular backend, and nothing here should grow
 * to imitate one. Where the contract leaves a choice open it takes the simple
 * option: a write to an unknown table creates it, partition columns are
 * ignored, and every {@link Options} key is ignored.
 *
 * <p>Constructed with or without namespaces, so the contract is exercised
 * both ways - the Hive-shaped reading and the index-only one Elasticsearch
 * will need.
 */
final class InMemoryDataStore implements DataStore {

    private static final long serialVersionUID = 1L;

    private final boolean namespaces;
    private final Map<TableRef, StoredTable> tables = new LinkedHashMap<TableRef, StoredTable>();
    private final Set<String> knownNamespaces = new LinkedHashSet<String>();

    /** A store with namespaces, like Hive. */
    InMemoryDataStore() {
        this(true);
    }

    /** @param namespaces {@code false} for an index-only store, like Elasticsearch */
    InMemoryDataStore(boolean namespaces) {
        this.namespaces = namespaces;
    }

    @Override
    public String name() {
        return namespaces ? "in-memory" : "in-memory (no namespaces)";
    }

    // ------------------------------------------------------------------- read

    @Override
    public synchronized Dataset<Row> read(SparkSession spark, TableRef table, Options options) {
        StoredTable stored = existing(table);
        return spark.createDataFrame(new ArrayList<Row>(stored.rows), stored.schema);
    }

    // ------------------------------------------------------------------ write

    @Override
    public synchronized void write(Dataset<Row> dataset, TableRef table, WriteMode mode, Options options) {
        StoredTable stored = tables.get(table);
        if (stored == null) {
            // The contract allows a backend to create implicitly; this one does.
            stored = new StoredTable(dataset.schema());
            tables.put(table, stored);
        }
        List<Row> incoming = dataset.collectAsList();

        switch (mode) {
            case APPEND:
                stored.rows.addAll(incoming);
                return;
            case OVERWRITE:
                stored.rows.clear();
                stored.rows.addAll(incoming);
                return;
            case ERROR_IF_EXISTS:
                if (!stored.rows.isEmpty()) {
                    throw new IoException(table + " already holds " + stored.rows.size()
                            + " row(s) and the write asked for " + mode);
                }
                stored.rows.addAll(incoming);
                return;
            case IGNORE:
                if (stored.rows.isEmpty()) {
                    stored.rows.addAll(incoming);
                }
                return;
            default:
                throw new IoException("unsupported write mode " + mode);
        }
    }

    // -------------------------------------------------------------------- ddl

    @Override
    public synchronized boolean namespaceExists(String namespace) {
        return namespaces && knownNamespaces.contains(namespace);
    }

    @Override
    public synchronized void createNamespace(String namespace, Options options) {
        if (namespaces && namespace != null) {
            knownNamespaces.add(namespace);
        }
    }

    @Override
    public synchronized void dropNamespace(String namespace, boolean cascade) {
        if (!namespaces || !knownNamespaces.contains(namespace)) {
            return;
        }
        List<TableRef> occupants = listTables(namespace);
        if (!occupants.isEmpty() && !cascade) {
            throw new IoException("namespace " + namespace + " still holds " + occupants.size()
                    + " table(s); drop it with cascade to remove them too");
        }
        for (TableRef occupant : occupants) {
            tables.remove(occupant);
        }
        knownNamespaces.remove(namespace);
    }

    @Override
    public synchronized List<String> listNamespaces() {
        return namespaces ? new ArrayList<String>(knownNamespaces) : new ArrayList<String>();
    }

    @Override
    public synchronized boolean tableExists(TableRef table) {
        return tables.containsKey(table);
    }

    @Override
    public synchronized void createTable(TableSpec spec) {
        TableRef table = spec.table();
        if (tables.containsKey(table)) {
            throw new IoException("table " + table + " already exists in the " + name() + " store");
        }
        if (namespaces && table.hasNamespace() && !knownNamespaces.contains(table.namespace())) {
            throw new IoException("no namespace " + table.namespace() + " to create " + table + " in");
        }
        // Partition columns are validated by TableSpec and have no meaning here.
        tables.put(table, new StoredTable(spec.schema()));
    }

    @Override
    public synchronized void dropTable(TableRef table) {
        tables.remove(table);
    }

    @Override
    public synchronized void truncateTable(TableRef table) {
        existing(table).rows.clear();
    }

    @Override
    public synchronized List<TableRef> listTables(String namespace) {
        List<TableRef> found = new ArrayList<TableRef>();
        for (TableRef table : tables.keySet()) {
            String owner = table.namespace();
            if (owner == null ? namespace == null : owner.equals(namespace)) {
                found.add(table);
            }
        }
        return found;
    }

    @Override
    public synchronized StructType schemaOf(TableRef table) {
        return existing(table).schema;
    }

    @Override
    public String toString() {
        return name() + " store, " + tables.size() + " table(s)";
    }

    private StoredTable existing(TableRef table) {
        StoredTable stored = tables.get(table);
        if (stored == null) {
            throw new IoException("no table " + table + " in the " + name() + " store");
        }
        return stored;
    }

    /** One table: a fixed schema and the rows written into it so far. */
    private static final class StoredTable {

        private final StructType schema;
        private final List<Row> rows = new ArrayList<Row>();

        private StoredTable(StructType schema) {
            this.schema = schema;
        }
    }
}
