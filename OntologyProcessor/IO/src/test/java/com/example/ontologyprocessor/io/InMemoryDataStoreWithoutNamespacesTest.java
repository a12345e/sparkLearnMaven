package com.example.ontologyprocessor.io;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Runs {@link DataStoreContract} against an {@link InMemoryDataStore} that has
 * no namespaces - the shape Elasticsearch will have, where an index is
 * addressed by a bare name.
 *
 * <p>The point is the contract, not this store. {@link DdlOperations} promises
 * that a backend without namespaces answers the namespace operations quietly
 * rather than throwing, so that a caller doing "make the namespace, then the
 * table" works against either kind. That promise is worth exercising now,
 * while it is still cheap to change, rather than discovering when the
 * Elasticsearch store is written that the interface never allowed for it.
 */
public class InMemoryDataStoreWithoutNamespacesTest extends DataStoreContract {

    @Override
    protected DataStore newStore() {
        return new InMemoryDataStore(false);
    }

    @Override
    protected boolean supportsNamespaces() {
        return false;
    }

    @Test
    public void tablesAreAddressedByABareName() {
        assertFalse(table("people").hasNamespace());
        assertEquals("people", table("people").qualifiedName());
    }

    @Test
    public void namespaceOperationsAreQuietRatherThanFatal() {
        store().createNamespace("ontology");
        store().createNamespace("ontology", Options.of("location", "/tmp"));
        store().dropNamespace("ontology", true);

        assertFalse(store().namespaceExists("ontology"));
        assertTrue(store().listNamespaces().isEmpty());
    }

    @Test
    public void aBareNameStillRoundTrips() {
        TableRef index = table("people");
        store().createTable(TableSpec.of(index, SCHEMA));

        store().overwrite(people(), index);

        assertData(people(), store().read(spark(), index));
    }
}
