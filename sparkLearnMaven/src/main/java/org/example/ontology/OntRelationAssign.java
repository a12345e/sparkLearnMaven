package org.example.ontology;

import java.util.Set;

public class OntRelationAssign {
    private final Set<OntEntityAssign> from;
    private final  OntEntityAssign to;
    private final OntRelation type;
    private final Set<OntPropertyAssign> properties;

    public final Set<OntEntityAssign> getFrom(){ return from;};
    public final  OntEntityAssign getTo(){ return to;};
    public final  OntRelation getType(){ return type;};
    public final Set<OntPropertyAssign> getProperties() { return properties;};

    public Set<String> getColumnNames(){
        Set<String>  columnNames  = to.getColumnNames();
        for (OntEntityAssign e: from){
            columnNames.addAll(e.getColumnNames());
        }
        for(OntPropertyAssign p: getProperties()){
           columnNames.add(p.getColumnName());
        }
        return columnNames;
    };

    public OntRelationAssign(Set<OntEntityAssign> from, OntEntityAssign to, OntRelation type, Set<OntPropertyAssign> properties){
        this.from = from;
        this.to = to;
        this.type = type;
        this.properties = properties;
    }
}
