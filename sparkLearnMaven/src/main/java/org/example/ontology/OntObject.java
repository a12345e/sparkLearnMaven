package org.example.ontology;
import org.apache.spark.sql.Column;
import scala.Tuple2;

import java.util.List;
import java.util.Map;

public class OntObject {
    private final OntObjectType type;
    private final OntObjectRole role;
    private final Map<OntObjectProperty, Column> properties;

    public  OntObjectType getType(){
        return type;
    };

    public OntObjectRole getRole(){
        return this.role;
    };
    public Map<OntObjectProperty, Column> getProperties(){
        return properties;
    }
    public OntObject(OntObjectType type,
                     OntObjectRole role,
                     Map<OntObjectProperty, Column> properties){
        this.type = type;
        this.role = role;
        this.properties = properties;
    }
}
