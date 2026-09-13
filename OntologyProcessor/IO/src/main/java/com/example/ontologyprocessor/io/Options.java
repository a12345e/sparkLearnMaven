package com.example.ontologyprocessor.io;

import java.io.Serializable;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Backend-specific settings for one call: an immutable, ordered map of strings.
 *
 * <p>Strings because that is what both sides already speak. Spark's
 * {@code DataFrameReader.options} and {@code DataFrameWriter.options} take a
 * string map, and so does every connector behind them - {@code compression}
 * and {@code mergeSchema} on Parquet, {@code es.mapping.id} and
 * {@code es.write.operation} on Elasticsearch. Passing them through untyped
 * keeps this package from having to know a single one of them.
 *
 * <p>That is deliberate, and it is the seam where backend knowledge is allowed
 * to leak: {@link TableRef}, {@link WriteMode} and {@link TableSpec} are the
 * portable part of a call, and anything only one backend understands goes
 * here. An implementation should ignore keys it does not recognise rather than
 * fail, so a caller can hand the same options to two stores.
 *
 * <pre>{@code
 * Options options = Options.of("compression", "snappy").with("mergeSchema", "true");
 * }</pre>
 *
 * <p>Every method returns a new instance; nothing mutates. Instances are
 * {@link Serializable} and are value-equal, so they are safe to capture in a
 * lambda and safe to compare in a test.
 */
public final class Options implements Serializable {

    private static final long serialVersionUID = 1L;

    private static final Options NONE = new Options(Collections.<String, String>emptyMap());

    private final Map<String, String> values;

    private Options(Map<String, String> values) {
        this.values = Collections.unmodifiableMap(values);
    }

    /** @return the shared empty instance */
    public static Options none() {
        return NONE;
    }

    public static Options of(String key, String value) {
        return NONE.with(key, value);
    }

    public static Options of(String key1, String value1, String key2, String value2) {
        return of(key1, value1).with(key2, value2);
    }

    /** Copies a map; the caller may keep mutating theirs. */
    public static Options from(Map<String, String> values) {
        if (values == null) {
            throw new IllegalArgumentException("values must not be null");
        }
        Map<String, String> copy = new LinkedHashMap<String, String>();
        for (Map.Entry<String, String> entry : values.entrySet()) {
            copy.put(requireKey(entry.getKey()), requireValue(entry.getValue(), entry.getKey()));
        }
        return copy.isEmpty() ? NONE : new Options(copy);
    }

    /** @return a copy with {@code key} set, replacing any existing value */
    public Options with(String key, String value) {
        Map<String, String> copy = new LinkedHashMap<String, String>(values);
        copy.put(requireKey(key), requireValue(value, key));
        return new Options(copy);
    }

    /** @return a copy without {@code key}; an absent key is not an error */
    public Options without(String key) {
        if (key == null || !values.containsKey(key.trim())) {
            return this;
        }
        Map<String, String> copy = new LinkedHashMap<String, String>(values);
        copy.remove(key.trim());
        return copy.isEmpty() ? NONE : new Options(copy);
    }

    /** @return this and {@code other} combined; {@code other} wins on a shared key */
    public Options merge(Options other) {
        if (other == null || other.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return other;
        }
        Map<String, String> copy = new LinkedHashMap<String, String>(values);
        copy.putAll(other.values);
        return new Options(copy);
    }

    /** @return the value, or {@code null} if absent */
    public String get(String key) {
        return key == null ? null : values.get(key.trim());
    }

    public String get(String key, String defaultValue) {
        String value = get(key);
        return value == null ? defaultValue : value;
    }

    /** @throws IllegalStateException if the key is absent */
    public String require(String key) {
        String value = get(key);
        if (value == null) {
            throw new IllegalStateException("missing option '" + key + "'; have " + keys());
        }
        return value;
    }

    public boolean contains(String key) {
        return get(key) != null;
    }

    /** @return every key present, sorted, for readable error messages */
    public Set<String> keys() {
        return Collections.unmodifiableSet(new TreeSet<String>(values.keySet()));
    }

    /** @return the settings as an unmodifiable map, in insertion order, ready for Spark */
    public Map<String, String> asMap() {
        return values;
    }

    public boolean isEmpty() {
        return values.isEmpty();
    }

    public int size() {
        return values.size();
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof Options && values.equals(((Options) other).values);
    }

    @Override
    public int hashCode() {
        return values.hashCode();
    }

    @Override
    public String toString() {
        return "Options" + values;
    }

    private static String requireKey(String key) {
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("option key must not be blank");
        }
        return key.trim();
    }

    private static String requireValue(String value, String key) {
        if (value == null) {
            throw new IllegalArgumentException("option '" + key + "' must not be null");
        }
        return value;
    }
}
