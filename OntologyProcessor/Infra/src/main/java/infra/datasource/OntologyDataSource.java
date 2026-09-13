package infra.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * One ontology data source: where it comes from, how it is extracted and
 * transformed, and the window of time it covers.
 *
 * <p>Instances are immutable, so a definition read once can be shared freely -
 * including into lambdas that Spark ships to executors.
 *
 * <p>Definitions are written as JSON by hand, so {@link #fromJson(String)} is
 * deliberately strict: an unknown field is almost always a typo in a hand-edited
 * file, and failing loudly beats silently dropping it.
 */
public class OntologyDataSource {

    /**
     * Shared reader. An ObjectMapper is costly to build and thread-safe once
     * configured, so it is built once here rather than per call.
     */
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final OntologyDataSourceKey key;
    private final String site;
    private final OntologyDataSourceExtract extract;
    private final String transform;
    private final String minimal_dt;
    private final String maximal_dt;
    private final OntologyDataSourceStatus status;

    @JsonCreator
    public OntologyDataSource(@JsonProperty("key") OntologyDataSourceKey key,
                              @JsonProperty("site") String site,
                              @JsonProperty("extract") OntologyDataSourceExtract extract,
                              @JsonProperty("transform") String transform,
                              @JsonProperty("minimal_dt") String minimal_dt,
                              @JsonProperty("maximal_dt") String maximal_dt,
                              @JsonProperty("status") OntologyDataSourceStatus status) {
        this.key = key;
        this.site = site;
        this.extract = extract;
        this.transform = transform;
        this.minimal_dt = minimal_dt;
        this.maximal_dt = maximal_dt;
        this.status = status;
    }

    /**
     * Reads a data source from its JSON form.
     *
     * <p>Property names are the field names of this class, with {@code key} and
     * {@code extract} as nested objects and {@code status} one of the
     * {@link OntologyDataSourceStatus} names. A field left out comes back null;
     * only malformed JSON, a bad status, or an unrecognised field is rejected.
     *
     * <pre>
     * {
     *   "key":     {"env": "test", "id": 1, "name": "orders",
     *               "product": "retail", "mission": "daily"},
     *   "site":    "tel-aviv",
     *   "extract": {"tableName": "raw.orders", "technology": "HIVE",
     *               "sql": "select * from raw.orders"},
     *   "transform":  "normalize_orders",
     *   "minimal_dt": "2024-01-01",
     *   "maximal_dt": "2024-12-31",
     *   "status":     "ACTIVE"
     * }
     * </pre>
     *
     * @param json the JSON text; must not be null
     * @return the data source it describes
     * @throws IllegalArgumentException if the text is null, is not JSON, or does
     *         not describe a data source
     */
    public static OntologyDataSource fromJson(String json) {
        if (json == null) {
            throw new IllegalArgumentException("no json to read a data source from");
        }
        try {
            return MAPPER.readValue(json, OntologyDataSource.class);
        } catch (JsonProcessingException e) {
            throw new IllegalArgumentException(
                    "not a valid ontology data source: " + e.getOriginalMessage(), e);
        }
    }

    public OntologyDataSourceKey getKey() {
        return key;
    }

    public String getSite() {
        return site;
    }

    public OntologyDataSourceExtract getExtract() {
        return extract;
    }

    public String getTransform() {
        return transform;
    }

    public String getMinimal_dt() {
        return minimal_dt;
    }

    public String getMaximal_dt() {
        return maximal_dt;
    }

    public OntologyDataSourceStatus getStatus() {
        return status;
    }

}
