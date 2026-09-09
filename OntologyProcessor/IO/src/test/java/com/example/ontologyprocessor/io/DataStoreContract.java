package com.example.ontologyprocessor.io;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * What every {@link DataStore} must do, written once.
 *
 * <p>This is the specification the interfaces cannot express.
 * {@link DataWriter} can declare that {@link WriteMode#OVERWRITE} replaces
 * rather than appends; only a test can hold a backend to it. Putting that test
 * here rather than beside each backend means Hive and Elasticsearch are held
 * to the same promises, and that a disagreement about what a promise means
 * surfaces as a failure rather than as two backends quietly behaving
 * differently.
 *
 * <p>Extend it and supply a store:
 *
 * <pre>{@code
 * public class HiveDataStoreTest extends DataStoreContract {
 *     protected DataStore newStore() { return new HiveDataStore(ClusterConfig.load()); }
 * }
 * }</pre>
 *
 * <p>A backend module reaches this class by depending on {@code com.example:IO}
 * with a {@code test-jar} type and {@code test} scope; the version is managed
 * by the parent POM.
 *
 * <p>A backend without namespaces - Elasticsearch - overrides
 * {@link #supportsNamespaces()} to return {@code false}. The namespace tests
 * then check the no-op behaviour {@link DdlOperations} promises instead of
 * being skipped, and tables are addressed by a bare name.
 *
 * <p>Subclasses may add tests for anything specific to their backend. They
 * should not override the ones here: a backend that cannot pass one has found
 * a disagreement worth settling in the interface, not in an override.
 */
public abstract class DataStoreContract extends SparkTestSupport {

    protected static final String SCHEMA = "id INT, name STRING";

    private DataStore store;

    /** @return a store to test, fresh and empty; called before every test */
    protected abstract DataStore newStore();

    /** @return whether this backend has namespaces; {@code false} for an index-only store */
    protected boolean supportsNamespaces() {
        return true;
    }

    /** @return the namespace the tests work in, or {@code null} when there are none */
    protected String namespace() {
        return supportsNamespaces() ? "io_contract" : null;
    }

    /** @return the store under test */
    protected final DataStore store() {
        return store;
    }

    /** @return a reference in the test namespace */
    protected final TableRef table(String name) {
        return TableRef.of(namespace(), name);
    }

    @Before
    public final void openStore() {
        store = newStore();
        if (store == null) {
            throw new IllegalStateException("newStore() returned null");
        }
        if (supportsNamespaces()) {
            store.createNamespace(namespace());
        }
    }

    /**
     * Leaves the backend as it was found.
     *
     * <p>Failures here are swallowed: a test that already failed should report
     * its own reason, not a cleanup error on the way out.
     */
    @After
    public final void dropWhatTheTestCreated() {
        if (store == null) {
            return;
        }
        try {
            for (TableRef table : store.listTables(namespace())) {
                store.dropTable(table);
            }
        } catch (RuntimeException ignored) {
            // The outcome of the test itself is the interesting one.
        }
        store = null;
    }

    // --------------------------------------------------------------- fixtures

    protected final Dataset<Row> people() {
        return df(SCHEMA, row(1, "ada"), row(2, "grace"), row(3, "alan"));
    }

    private TableSpec spec(TableRef table) {
        return TableSpec.of(table, SCHEMA);
    }

    private TableRef created(String name) {
        TableRef table = table(name);
        store().createTable(spec(table));
        return table;
    }

    // -------------------------------------------------------------------- ddl

    @Test
    public void aTableDoesNotExistUntilItIsCreated() {
        assertFalse(store().tableExists(table("absent")));
    }

    @Test
    public void createTableMakesTheTableExist() {
        TableRef table = created("created");

        assertTrue(store().tableExists(table));
    }

    @Test
    public void aCreatedTableHasTheDeclaredSchema() {
        TableRef table = created("declared_schema");

        assertEquals(StructType.fromDDL(SCHEMA), store().schemaOf(table));
    }

    @Test
    public void aCreatedTableIsEmpty() {
        TableRef table = created("still_empty");

        assertRowCount(0, store().read(spark(), table));
    }

    @Test
    public void creatingATableTwiceIsAnError() {
        TableRef table = created("twice");

        try {
            store().createTable(spec(table));
            fail("expected creating " + table + " twice to fail");
        } catch (IoException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(table.name()));
        }
    }

    @Test
    public void createTableIfAbsentIsSafeToRepeat() {
        TableRef table = table("if_absent");

        store().createTableIfAbsent(spec(table));
        store().createTableIfAbsent(spec(table));

        assertTrue(store().tableExists(table));
    }

    @Test
    public void dropTableRemovesIt() {
        TableRef table = created("dropped");

        store().dropTable(table);

        assertFalse(store().tableExists(table));
    }

    @Test
    public void droppingAnAbsentTableIsNotAnError() {
        store().dropTable(table("never_there"));
    }

    @Test
    public void listTablesReportsWhatWasCreated() {
        TableRef first = created("listed_one");
        TableRef second = created("listed_two");

        List<TableRef> listed = store().listTables(namespace());

        assertTrue(listed + " should contain " + first, listed.contains(first));
        assertTrue(listed + " should contain " + second, listed.contains(second));
    }

    @Test
    public void aDroppedTableIsNoLongerListed() {
        TableRef table = created("unlisted");
        store().dropTable(table);

        assertFalse(store().listTables(namespace()).contains(table));
    }

    @Test
    public void theSchemaOfAnAbsentTableIsAnError() {
        try {
            store().schemaOf(table("no_schema"));
            fail("expected schemaOf on an absent table to fail");
        } catch (IoException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("no_schema"));
        }
    }

    // ----------------------------------------------------------- read / write

    @Test
    public void whatIsWrittenCanBeReadBack() {
        TableRef table = created("round_trip");

        store().write(people(), table, WriteMode.APPEND);

        assertData(people(), store().read(spark(), table));
    }

    @Test
    public void aRoundTripPreservesTheSchema() {
        TableRef table = created("round_trip_schema");

        store().write(people(), table, WriteMode.APPEND);

        assertSchema(SCHEMA, store().read(spark(), table));
    }

    @Test
    public void appendAddsToWhatIsAlreadyThere() {
        TableRef table = created("appended");

        store().append(people(), table);
        store().append(df(SCHEMA, row(9, "hopper")), table);

        assertData(df(SCHEMA,
                row(1, "ada"), row(2, "grace"), row(3, "alan"), row(9, "hopper")),
                store().read(spark(), table));
    }

    @Test
    public void overwriteReplacesWhatIsAlreadyThere() {
        TableRef table = created("overwritten");

        store().append(people(), table);
        store().overwrite(df(SCHEMA, row(9, "hopper")), table);

        assertData(df(SCHEMA, row(9, "hopper")), store().read(spark(), table));
    }

    @Test
    public void overwritingAnEmptyTableJustWrites() {
        TableRef table = created("overwrite_empty");

        store().overwrite(people(), table);

        assertData(people(), store().read(spark(), table));
    }

    @Test
    public void errorIfExistsWritesIntoAnEmptyTable() {
        TableRef table = created("error_empty");

        store().write(people(), table, WriteMode.ERROR_IF_EXISTS);

        assertData(people(), store().read(spark(), table));
    }

    @Test
    public void errorIfExistsRefusesToTouchRows() {
        TableRef table = created("error_nonempty");
        store().append(people(), table);

        try {
            store().write(df(SCHEMA, row(9, "hopper")), table, WriteMode.ERROR_IF_EXISTS);
            fail("expected ERROR_IF_EXISTS to refuse a table that holds rows");
        } catch (IoException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(table.name()));
        }

        assertData(people(), store().read(spark(), table));
    }

    @Test
    public void ignoreLeavesExistingRowsAlone() {
        TableRef table = created("ignored");
        store().append(people(), table);

        store().write(df(SCHEMA, row(9, "hopper")), table, WriteMode.IGNORE);

        assertData(people(), store().read(spark(), table));
    }

    @Test
    public void ignoreWritesIntoAnEmptyTable() {
        TableRef table = created("ignore_empty");

        store().write(people(), table, WriteMode.IGNORE);

        assertData(people(), store().read(spark(), table));
    }

    @Test
    public void anEmptyDatasetCanBeWritten() {
        TableRef table = created("write_empty");

        store().write(emptyDf(SCHEMA), table, WriteMode.APPEND);

        Dataset<Row> read = store().read(spark(), table);
        assertRowCount(0, read);
        assertSchema(SCHEMA, read);
    }

    @Test
    public void overwritingWithNothingEmptiesTheTable() {
        TableRef table = created("overwrite_with_empty");
        store().append(people(), table);

        store().overwrite(emptyDf(SCHEMA), table);

        assertRowCount(0, store().read(spark(), table));
    }

    @Test
    public void readingAnAbsentTableIsAnError() {
        try {
            store().read(spark(), table("unread")).count();
            fail("expected reading an absent table to fail");
        } catch (IoException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("unread"));
        }
    }

    @Test
    public void truncateEmptiesTheTableButKeepsIt() {
        TableRef table = created("truncated");
        store().append(people(), table);

        store().truncateTable(table);

        assertTrue("still there", store().tableExists(table));
        assertRowCount(0, store().read(spark(), table));
        assertEquals("schema kept", StructType.fromDDL(SCHEMA), store().schemaOf(table));
    }

    @Test
    public void aTruncatedTableCanBeWrittenAgain() {
        TableRef table = created("truncate_rewrite");
        store().append(people(), table);
        store().truncateTable(table);

        store().append(df(SCHEMA, row(9, "hopper")), table);

        assertData(df(SCHEMA, row(9, "hopper")), store().read(spark(), table));
    }

    @Test
    public void truncatingAnAbsentTableIsAnError() {
        try {
            store().truncateTable(table("untruncatable"));
            fail("expected truncating an absent table to fail");
        } catch (IoException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("untruncatable"));
        }
    }

    @Test
    public void twoTablesDoNotSeeEachOtherRows() {
        TableRef first = created("separate_one");
        TableRef second = created("separate_two");

        store().append(people(), first);
        store().append(df(SCHEMA, row(9, "hopper")), second);

        assertData(people(), store().read(spark(), first));
        assertData(df(SCHEMA, row(9, "hopper")), store().read(spark(), second));
    }

    // ------------------------------------------------------------- namespaces

    @Test
    public void aStoreReportsTheNamespaceTheTestsUse() {
        if (!supportsNamespaces()) {
            assertFalse("a store without namespaces reports none exist",
                    store().namespaceExists("io_contract"));
            assertTrue("and lists none", store().listNamespaces().isEmpty());
            return;
        }
        assertTrue(store().namespaceExists(namespace()));
        assertTrue(store().listNamespaces() + " should contain " + namespace(),
                store().listNamespaces().contains(namespace()));
    }

    @Test
    public void creatingANamespaceIsSafeToRepeat() {
        if (!supportsNamespaces()) {
            store().createNamespace("anything");
            assertFalse("still no namespaces", store().namespaceExists("anything"));
            return;
        }
        store().createNamespace(namespace());
        store().createNamespace(namespace());

        assertTrue(store().namespaceExists(namespace()));
    }

    @Test
    public void anAbsentNamespaceDoesNotExist() {
        assertFalse(store().namespaceExists("io_contract_absent"));
    }

    @Test
    public void anEmptyNamespaceCanBeDropped() {
        if (!supportsNamespaces()) {
            store().dropNamespace("anything", false);
            return;
        }
        store().createNamespace("io_contract_spare");
        store().dropNamespace("io_contract_spare", false);

        assertFalse(store().namespaceExists("io_contract_spare"));
    }

    @Test
    public void droppingAnAbsentNamespaceIsNotAnError() {
        store().dropNamespace("io_contract_absent", false);
    }

    @Test
    public void aNamespaceHoldingTablesNeedsCascade() {
        if (!supportsNamespaces()) {
            return;
        }
        created("occupant");

        try {
            store().dropNamespace(namespace(), false);
            fail("expected dropping a non-empty namespace without cascade to fail");
        } catch (IoException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains(namespace()));
        }
        assertTrue("nothing was dropped", store().namespaceExists(namespace()));
    }

    @Test
    public void cascadeDropsTheTablesToo() {
        if (!supportsNamespaces()) {
            return;
        }
        TableRef table = created("cascaded");

        store().dropNamespace(namespace(), true);

        assertFalse(store().namespaceExists(namespace()));
        assertFalse(store().tableExists(table));
    }

    // ------------------------------------------------------------------- misc

    @Test
    public void aStoreSaysWhatItIs() {
        String name = store().name();

        assertTrue("name must not be blank", name != null && !name.trim().isEmpty());
    }
}
