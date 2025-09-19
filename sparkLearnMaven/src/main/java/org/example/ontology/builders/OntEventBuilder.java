package org.example.ontology.builders;

import org.apache.spark.sql.Column;
import org.example.ontology.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OntEventBuilder {
    private final Map<OntEventProperty, Column> properties = new HashMap<>();
    private final List<OntObject> objects = new ArrayList<>();
    private final List<OntRelation> relations = new ArrayList<>();
    private OntEventType type;

    public OntEventBuilder relation(OntRelation relation){
        relations.add(relation);
        return this;
    }
    public OntEventBuilder object(OntObject object){
        objects.add(object);
        return this;
    }
    public OntEventBuilder property(OntEventProperty name, Column value){
        properties.put(name, value);
        return this;
    }
    public OntEventBuilder type(OntEventType type){
        this.type = type;
        return this;
    }
    public OntEvent build(){
        return new OntEvent(type, properties, objects, relations);
    }

}
