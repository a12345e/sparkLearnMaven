package org.example.ontology.builders;

import org.apache.spark.sql.Column;
import org.example.ontology.*;

import java.util.HashMap;
import java.util.Map;

public class OntRelationBuilder {
    private  OntObject source;
    private  OntObject destination;
    private OntRelationType type;
    public OntRelationBuilder source(OntObject source){
        this.source = source;
        return this;
    }
    public OntRelationBuilder destination(OntObject destination){
        this.destination = destination;
        return this;
    }
    public OntRelationBuilder type(OntRelationType type){
        this.type = type;
        return this;
    }
    public OntRelation build(){
        return new OntRelation(source,destination,type);
    }

}
