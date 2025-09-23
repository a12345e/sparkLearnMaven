package org.example.ontology;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class OntRelation implements Normalize{
    private final OntEntity source;
    private final OntEntity destination;
    public OntEntity getSource() { return source;};
    public OntEntity getDestination(){ return destination;};
    public OntRelation(OntEntity source,OntEntity destination){
        this.source = source;
        this.destination  = destination;
    }
    public Dataset<Row> normalize(Dataset<Row> data){
        return data;
    }

    public abstract OntRole getType();

}
