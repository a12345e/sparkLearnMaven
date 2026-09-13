package com.example.ontologyprocessor.io;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Specifies {@link Options}: immutable, ordered, and safe to share.
 *
 * <p>Immutability is the property worth testing hardest. Options are captured
 * in lambdas that Spark ships to executors and are meant to be built once and
 * handed to several stores, so a method that mutated in place rather than
 * returning a copy would be a bug that only showed up under concurrency.
 */
public class OptionsTest {

    @Test
    public void noneIsEmptyAndShared() {
        assertTrue(Options.none().isEmpty());
        assertEquals(0, Options.none().size());
        assertSame(Options.none(), Options.none());
        assertTrue(Options.none().asMap().isEmpty());
    }

    @Test
    public void aSettingCanBeReadBack() {
        Options options = Options.of("compression", "snappy");

        assertEquals("snappy", options.get("compression"));
        assertTrue(options.contains("compression"));
        assertEquals(1, options.size());
        assertFalse(options.isEmpty());
    }

    @Test
    public void twoSettingsCanBeGivenAtOnce() {
        Options options = Options.of("compression", "snappy", "mergeSchema", "true");

        assertEquals("snappy", options.get("compression"));
        assertEquals("true", options.get("mergeSchema"));
    }

    @Test
    public void anAbsentSettingIsNullOrTheDefault() {
        Options options = Options.of("compression", "snappy");

        assertNull(options.get("mergeSchema"));
        assertFalse(options.contains("mergeSchema"));
        assertEquals("false", options.get("mergeSchema", "false"));
        assertEquals("snappy", options.get("compression", "gzip"));
    }

    @Test
    public void requireNamesTheMissingKeyAndWhatWasThere() {
        try {
            Options.of("compression", "snappy").require("es.mapping.id");
            fail("expected a missing option to be reported");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage(), expected.getMessage().contains("es.mapping.id"));
            assertTrue(expected.getMessage(), expected.getMessage().contains("compression"));
        }
    }

    @Test
    public void withReturnsACopyAndLeavesTheOriginalAlone() {
        Options original = Options.of("compression", "snappy");
        Options extended = original.with("mergeSchema", "true");

        assertEquals("original untouched", 1, original.size());
        assertNull("original untouched", original.get("mergeSchema"));
        assertEquals("copy has both", 2, extended.size());
    }

    @Test
    public void withReplacesAnExistingValue() {
        Options options = Options.of("compression", "snappy").with("compression", "gzip");

        assertEquals("gzip", options.get("compression"));
        assertEquals(1, options.size());
    }

    @Test
    public void withoutRemovesASettingAndIgnoresAnAbsentOne() {
        Options options = Options.of("compression", "snappy", "mergeSchema", "true");

        assertEquals(Options.of("mergeSchema", "true"), options.without("compression"));
        assertSame("nothing to remove", options, options.without("nope"));
        assertSame("nothing to remove", options, options.without(null));
        assertTrue(Options.of("a", "1").without("a").isEmpty());
    }

    @Test
    public void mergeLetsTheOtherSideWin() {
        Options defaults = Options.of("compression", "snappy", "mergeSchema", "true");
        Options overrides = Options.of("compression", "gzip");

        Options merged = defaults.merge(overrides);

        assertEquals("gzip", merged.get("compression"));
        assertEquals("true", merged.get("mergeSchema"));
        assertEquals("defaults untouched", "snappy", defaults.get("compression"));
    }

    @Test
    public void mergingWithNothingChangesNothing() {
        Options options = Options.of("compression", "snappy");

        assertSame(options, options.merge(Options.none()));
        assertSame(options, options.merge(null));
        assertSame(options, Options.none().merge(options));
    }

    @Test
    public void insertionOrderIsKeptForTheBackend() {
        Options options = Options.of("a", "1").with("b", "2").with("c", "3");

        assertEquals(Arrays.asList("a", "b", "c"), new ArrayList<String>(options.asMap().keySet()));
    }

    @Test
    public void keysAreSortedForReadableMessages() {
        Options options = Options.of("c", "3").with("a", "1").with("b", "2");

        assertEquals(Arrays.asList("a", "b", "c"), new ArrayList<String>(options.keys()));
    }

    @Test
    public void fromCopiesTheCallersMap() {
        Map<String, String> source = new LinkedHashMap<String, String>();
        source.put("compression", "snappy");
        Options options = Options.from(source);

        source.put("mergeSchema", "true");

        assertEquals("the copy did not follow the source", 1, options.size());
        assertNull(options.get("mergeSchema"));
    }

    @Test
    public void fromAnEmptyMapIsNone() {
        assertSame(Options.none(), Options.from(new LinkedHashMap<String, String>()));
    }

    @Test
    public void theExposedMapCannotBeMutated() {
        Options options = Options.of("compression", "snappy");
        try {
            options.asMap().put("mergeSchema", "true");
            fail("expected asMap() to be unmodifiable");
        } catch (UnsupportedOperationException expected) {
            // The point of the test.
        }
        try {
            options.keys().add("mergeSchema");
            fail("expected keys() to be unmodifiable");
        } catch (UnsupportedOperationException expected) {
            // The point of the test.
        }
    }

    @Test
    public void whitespaceAroundAKeyIsNotPartOfIt() {
        Options options = Options.of("  compression  ", "snappy");

        assertEquals("snappy", options.get("compression"));
        assertEquals("snappy", options.get(" compression "));
    }

    @Test
    public void aBlankKeyOrNullValueIsRejected() {
        assertRejected(null, "value");
        assertRejected("", "value");
        assertRejected("   ", "value");
        assertRejected("key", null);
    }

    @Test
    public void anEmptyValueIsAllowed() {
        assertEquals("", Options.of("es.mapping.exclude", "").get("es.mapping.exclude"));
    }

    @Test
    public void sameSettingsMeansEqual() {
        Options first = Options.of("a", "1").with("b", "2");
        Options second = Options.of("b", "2").with("a", "1");

        assertEquals("order is not identity", first, second);
        assertEquals(first.hashCode(), second.hashCode());
        assertFalse(first.equals(Options.of("a", "1")));
    }

    private static void assertRejected(String key, String value) {
        try {
            Options.of(key, value);
            fail("expected key '" + key + "' value '" + value + "' to be rejected");
        } catch (IllegalArgumentException expected) {
            // The point of the test.
        }
    }
}
