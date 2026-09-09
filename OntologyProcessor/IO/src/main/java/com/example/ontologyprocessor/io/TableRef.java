package com.example.ontologyprocessor.io;

import java.io.Serializable;

/**
 * What a read, a write or a DDL statement is addressed to: an optional
 * namespace and a name.
 *
 * <p>One type covers every backend because every backend this project targets
 * addresses data the same way. On Hive the namespace is the database and the
 * name is the table; on Elasticsearch there is usually no namespace and the
 * name is the index. Nothing here knows which - that is the point, and it is
 * what lets a {@link DataStore} be swapped without touching a caller.
 *
 * <pre>{@code
 * TableRef people  = TableRef.of("ontology", "people");   // ontology.people
 * TableRef index   = TableRef.of("people-2026");          // no namespace
 * TableRef parsed  = TableRef.parse("ontology.people");   // same as the first
 * }</pre>
 *
 * <p>Immutable, {@link Serializable} and a proper value: two refs naming the
 * same thing are {@code equals}, so a ref is safe as a map key and safe to
 * capture in a lambda shipped to the executors.
 */
public final class TableRef implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String namespace;
    private final String name;

    private TableRef(String namespace, String name) {
        this.namespace = namespace;
        this.name = name;
    }

    /** A name with no namespace - an Elasticsearch index, or a Hive table in the session's current database. */
    public static TableRef of(String name) {
        return new TableRef(null, require(name, "name"));
    }

    /**
     * A name within a namespace.
     *
     * @param namespace the Hive database, or {@code null} for none
     */
    public static TableRef of(String namespace, String name) {
        return new TableRef(namespace == null ? null : require(namespace, "namespace"), require(name, "name"));
    }

    /**
     * Parses {@code namespace.name}, or a bare {@code name} when there is no dot.
     *
     * <p>Splits on the <em>first</em> dot, which is what a Hive identifier
     * needs. A bare name that itself contains dots - an Elasticsearch index
     * like {@code logs-2026.01.01} - must go through {@link #of(String)}
     * instead, or the leading segment would be read as a namespace.
     */
    public static TableRef parse(String qualifiedName) {
        String text = require(qualifiedName, "qualifiedName");
        int dot = text.indexOf('.');
        if (dot < 0) {
            return of(text);
        }
        if (dot == 0 || dot == text.length() - 1) {
            throw new IllegalArgumentException("not a qualified name: '" + qualifiedName + "'");
        }
        return of(text.substring(0, dot), text.substring(dot + 1));
    }

    /** @return the namespace, or {@code null} if there is none */
    public String namespace() {
        return namespace;
    }

    public String name() {
        return name;
    }

    public boolean hasNamespace() {
        return namespace != null;
    }

    /** @return {@code namespace.name}, or just {@code name} when there is no namespace */
    public String qualifiedName() {
        return namespace == null ? name : namespace + "." + name;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof TableRef)) {
            return false;
        }
        TableRef that = (TableRef) other;
        return name.equals(that.name)
                && (namespace == null ? that.namespace == null : namespace.equals(that.namespace));
    }

    @Override
    public int hashCode() {
        return 31 * (namespace == null ? 0 : namespace.hashCode()) + name.hashCode();
    }

    @Override
    public String toString() {
        return qualifiedName();
    }

    private static String require(String value, String what) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(what + " must not be blank");
        }
        return value.trim();
    }
}
