package org.example.ontology;

import org.apache.spark.sql.Column;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OntEvent {
    private final Map<OntEventProperty, Column> properties;
    private final List<OntObject> objects;
    private final List<OntRelation> relations;
    private final OntEventType type;

    public Map<OntEventProperty, Column> getProperties(){
        return properties;
    }
    private  List<OntObject> getObjects(){
        return objects;
    };
    private List<OntRelation> getRelations(){
        return relations;
    }
    private final OntEventType getType(){
        return type;
    }
    public OntEvent(
            OntEventType type,
            Map<OntEventProperty, Column> properties,
                    List<OntObject> objects,
                    List<OntRelation> relations){
        this.properties = properties;
        this.objects = objects;
        this.relations = relations;
        this.type = type;
    }
}
