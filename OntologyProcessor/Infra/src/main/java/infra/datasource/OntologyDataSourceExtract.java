package infra.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OntologyDataSourceExtract {
    /** Not part of the JSON or Spark form: a constant, not a field of the definition. */
    @JsonIgnore
    public final String HIVE = "HIVE";
    private final String  tableName;
    private final String technology;
    private final String sql;

    @JsonCreator
    public OntologyDataSourceExtract(@JsonProperty("tableName") String tableName,
                                     @JsonProperty("technology") String technology,
                                     @JsonProperty("sql") String sql) {
        this.tableName = tableName;
        this.technology = technology;
        this.sql = sql;
    }

    public String getTableName() {
        return tableName;
    }

    public String getTechnology() {
        return technology;
    }

    public String getSql() {
        return sql;
    }

}
