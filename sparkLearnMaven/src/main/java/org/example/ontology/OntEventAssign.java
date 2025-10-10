package org.example.ontology;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class OntEventAssign {
    public final OntEvent getEvent(){ return event;};
    public final OntTimeAssign getTime() { return time;}
    public final Set<OntPropertyAssign> getProperties() { return properties;}
    public final Set<OntRelationAssign> getRelations(){ return relations;};
    private final OntEvent event;
    private final OntTimeAssign time;
    private final Set<OntPropertyAssign> properties;
    private final Set<OntRelationAssign> relations;
    private void validate(){
        if (relations.isEmpty()){
            throw new RuntimeException("No relations");
        }
        Set<String> allProperties = new HashSet<>();
        for(OntPropertyAssign p: properties){
            if (!allProperties.add(p.getColumnName())){
                throw new RuntimeException("twice same column used, copy the column instead");
            }
        }
        for(OntRelationAssign r: relations){
            for(String columnName: r.getColumnNames()){
                if (!allProperties.add(columnName)){
                    throw new RuntimeException("twice same column used, copy the column instead");
                }

            }
        }
    }
    public OntEventAssign(OntEvent event, OntTimeAssign time, Set<OntPropertyAssign> properties,
                          Set<OntRelationAssign> relations){
        this.event = event;
        this.properties = properties;
        this.relations = relations;
        this.time = time;
        validate();
    }

}
