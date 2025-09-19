package org.example.ontology.builders;

import org.apache.spark.sql.Column;
import org.example.ontology.OntObject;
import org.example.ontology.OntObjectProperty;
import org.example.ontology.OntObjectRole;
import org.example.ontology.OntObjectType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OntObjectBuilder {
    private  OntObjectType type;
    private  OntObjectRole role;
    private  String separator;
    private final Map<OntObjectProperty, Column> properties = new HashMap<>();
    public  String getSeparator() {return separator;};
    public OntObjectBuilder separator(String separator){
        this.separator = separator;
        return this;
    }
    public OntObjectBuilder type(OntObjectType type){
        this.type = type;
        return this;
    }
    public OntObjectBuilder role(OntObjectRole role){
        this.role = role;
        return this;
    }
    public OntObjectBuilder property(OntObjectProperty name,Column value){
        properties.put(name, value);
        return this;
    }
    public OntObject build(){
        return new OntObject(type,role,properties);
    }

}
