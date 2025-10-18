package org.example.ontology;


import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class OntEntityAssign {
    private final OntEntity entity;
    private final Set<OntPropertyAssign> mandatoryAssignSet;
    private final Set<OntPropertyAssign> optionalAssignSet;
    private final OntRole role;
    public final OntEntity getEntity() { return entity;};
    public final Set<OntPropertyAssign> getMandatoryPropertiesAssignments() { return mandatoryAssignSet;};
    public final Set<OntPropertyAssign> getOptionalPropertiesAssignments(){return optionalAssignSet;};
    public OntRole getRole(){ return role;}

    public Set<String> getMandatoryColumnNames(){
        return mandatoryAssignSet.stream().map(OntPropertyAssign::getColumnName).collect(Collectors.toSet());
    }

    public Set<String> getColumnNames(){
        Set<String>  columnNames =  mandatoryAssignSet.stream().map(OntPropertyAssign::getColumnName).collect(Collectors.toSet());
        columnNames.addAll(optionalAssignSet.stream().map(OntPropertyAssign::getColumnName).collect(Collectors.toSet()));
        return columnNames;
    }

    public OntEntityAssign(OntEntity entity, OntRole role,
                            Collection<OntPropertyAssign> assignedProperties){
        Set<OntProperty> properties = assignedProperties.stream().map(OntPropertyAssign::getType).collect(Collectors.toSet());
        if(!properties.containsAll(entity.getMandatoryProperties())){
            throw new RuntimeException("Missing mandatory properties in provided properties:: " +properties);
        }
        properties.removeAll(new HashSet<>(entity.getMandatoryProperties()));
        properties.removeAll(new HashSet<>(entity.getOptionalProperties()));
        if(!properties.isEmpty()){
            throw new RuntimeException("Defined invalid properties for this entity "+properties);
        }
        this.entity = entity;
        this.mandatoryAssignSet = assignedProperties.stream().filter(x -> entity.getMandatoryProperties().contains(x.getType())).collect(Collectors.toSet());
        this.optionalAssignSet = assignedProperties.stream().filter(x -> entity.getOptionalProperties().contains(x.getType())).collect(Collectors.toSet());
        this.role = role;
    }
}
