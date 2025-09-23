package org.example.ontology;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.util.List;

public  abstract class OntEntity implements Normalize {
    private final OntRole role;
    private final List<OntProperty> properties;
    public OntRole getRole(){
        return this.role;
    };
    @Override
    public Dataset<Row> normalize(Dataset<Row> data){
        for(OntProperty p: properties){
            data = p.normalize(data);
        }
        return data;
    }

    public abstract OntEntityType getType();

    public List<OntProperty> getProperties(){
        return properties;
    }
    public OntEntity(OntRole role,
                    List<OntProperty> properties){
        this.role = role;
        this.properties = properties;
    }
}
