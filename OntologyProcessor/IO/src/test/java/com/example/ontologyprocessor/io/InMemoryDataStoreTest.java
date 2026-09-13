package com.example.ontologyprocessor.io;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Runs {@link DataStoreContract} against {@link InMemoryDataStore}, in its
 * Hive-shaped form: a store with namespaces.
 *
 * <p>Nothing here is about the in-memory store as such. It is the check that
 * the contract can be satisfied at all - that its tests do not contradict one
 * another, and that a backend author starting from them has a target that is
 * known to be reachable.
 *
 * <p>The few tests below cover what the contract deliberately leaves open, so
 * that the choices this store makes are written down rather than merely
 * happening.
 */
public class InMemoryDataStoreTest extends DataStoreContract {

    @Override
    protected DataStore newStore() {
        return new InMemoryDataStore();
    }

    @Test
    public void aWriteToAnUnknownTableCreatesIt() {
        TableRef table = table("implicit");

        store().append(people(), table);

        assertTrue(store().tableExists(table));
        assertData(people(), store().read(spark(), table));
    }

    @Test
    public void aTableCannotBeCreatedInAnAbsentNamespace() {
        TableSpec spec = TableSpec.of(TableRef.of("io_contract_absent", "orphan"), SCHEMA);

        try {
            store().createTable(spec);
            fail("expected creating a table in an absent namespace to fail");
        } catch (IoException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("io_contract_absent"));
        }
    }

    @Test
    public void aStoreSaysHowManyTablesItHolds() {
        store().createTable(TableSpec.of(table("counted"), SCHEMA));

        assertTrue(store().toString(), store().toString().contains("1 table(s)"));
    }

    @Test
    public void namespacesAreListedInTheOrderTheyWereCreated() {
        store().createNamespace("io_contract_b");
        store().createNamespace("io_contract_a");

        assertEquals("[io_contract, io_contract_b, io_contract_a]", store().listNamespaces().toString());
    }
}
