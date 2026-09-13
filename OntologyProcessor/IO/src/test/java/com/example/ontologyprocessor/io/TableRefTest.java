package com.example.ontologyprocessor.io;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** Specifies {@link TableRef}: how a target is named, and what is not a name. */
public class TableRefTest {

    @Test
    public void aNameCanStandAlone() {
        TableRef ref = TableRef.of("people");

        assertNull("namespace", ref.namespace());
        assertEquals("name", "people", ref.name());
        assertFalse("hasNamespace", ref.hasNamespace());
        assertEquals("qualifiedName", "people", ref.qualifiedName());
    }

    @Test
    public void aNameCanSitInANamespace() {
        TableRef ref = TableRef.of("ontology", "people");

        assertEquals("namespace", "ontology", ref.namespace());
        assertEquals("name", "people", ref.name());
        assertTrue("hasNamespace", ref.hasNamespace());
        assertEquals("qualifiedName", "ontology.people", ref.qualifiedName());
    }

    @Test
    public void anExplicitlyNullNamespaceMeansNone() {
        assertEquals(TableRef.of("people"), TableRef.of(null, "people"));
    }

    @Test
    public void parseSplitsOnTheFirstDot() {
        assertEquals(TableRef.of("ontology", "people"), TableRef.parse("ontology.people"));
    }

    @Test
    public void parseLeavesAnUnqualifiedNameAlone() {
        assertEquals(TableRef.of("people"), TableRef.parse("people"));
    }

    /**
     * The first dot, not the last: {@code a.b.c} is table {@code b.c} in
     * database {@code a}. An Elasticsearch index whose own name has dots must
     * go through {@code of}, which is what the javadoc says and what this
     * pins down.
     */
    @Test
    public void parseTreatsLaterDotsAsPartOfTheName() {
        TableRef ref = TableRef.parse("logs.2026.01.01");

        assertEquals("namespace", "logs", ref.namespace());
        assertEquals("name", "2026.01.01", ref.name());

        TableRef whole = TableRef.of("logs.2026.01.01");
        assertFalse("of() keeps the dots in the name", whole.hasNamespace());
        assertEquals("logs.2026.01.01", whole.name());
    }

    @Test
    public void surroundingWhitespaceIsNotPartOfAName() {
        assertEquals(TableRef.of("ontology", "people"), TableRef.of("  ontology ", " people  "));
    }

    @Test
    public void aBlankNameIsRejected() {
        assertRejects(null);
        assertRejects("");
        assertRejects("   ");
    }

    @Test
    public void aBlankNamespaceIsRejectedButAnAbsentOneIsNot() {
        try {
            TableRef.of("   ", "people");
            fail("expected a blank namespace to be rejected");
        } catch (IllegalArgumentException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("namespace"));
        }
    }

    @Test
    public void aDanglingDotIsNotAQualifiedName() {
        assertRejectsParse(".people");
        assertRejectsParse("ontology.");
    }

    @Test
    public void refsNamingTheSameThingAreEqualAndUsableAsAKey() {
        TableRef first = TableRef.parse("ontology.people");
        TableRef second = TableRef.of("ontology", "people");

        assertEquals(first, second);
        assertEquals(first.hashCode(), second.hashCode());

        Map<TableRef, String> byTable = new HashMap<TableRef, String>();
        byTable.put(first, "rows");
        assertEquals("rows", byTable.get(second));
    }

    @Test
    public void aNamespaceIsPartOfIdentity() {
        assertFalse(TableRef.of("people").equals(TableRef.of("ontology", "people")));
        assertFalse(TableRef.of("a", "people").equals(TableRef.of("b", "people")));
    }

    @Test
    public void toStringIsTheQualifiedName() {
        assertEquals("ontology.people", TableRef.parse("ontology.people").toString());
        assertEquals("people", TableRef.of("people").toString());
    }

    private static void assertRejects(String name) {
        try {
            TableRef.of(name);
            fail("expected '" + name + "' to be rejected");
        } catch (IllegalArgumentException expected) {
            // The point of the test.
        }
    }

    private static void assertRejectsParse(String qualifiedName) {
        try {
            TableRef.parse(qualifiedName);
            fail("expected '" + qualifiedName + "' to be rejected");
        } catch (IllegalArgumentException expected) {
            // The point of the test.
        }
    }
}
