package infra.datasource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class OntologyDataSourceKey {
    final private String env; // test , product, ..
    final private int id; // unique globally, starting with 1,2,..
    final private String name;
    final private String product;
    final private String mission;

    @JsonCreator
    public OntologyDataSourceKey(@JsonProperty("env") String env,
                                 @JsonProperty("id") int id,
                                 @JsonProperty("name") String name,
                                 @JsonProperty("product") String product,
                                 @JsonProperty("mission") String mission) {
        this.env = env;
        this.id = id;
        this.name = name;
        this.product = product;
        this.mission = mission;
    }

    public String getDataSourceName() {
        return id+"_"+name+"_"+product+"_"+mission;
    }

    public String getEnv() {
        return env;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getProduct() {
        return product;
    }

    public String getMission() {
        return mission;
    }

}
