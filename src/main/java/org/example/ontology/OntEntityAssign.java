package org.example.ontology;


import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class OntEntityAssign {
    private final OntEntity entity;
    private final Set<OntPropertyAssign> mAssignSet;
    private final Set<OntPropertyAssign> oAssignSet;
    private final OntRole role;
    public final OntEntity getEntity() { return entity;};
    public final Set<OntPropertyAssign> getMandatoryPropertiesAssignments() { return mAssignSet;};
    public final Set<OntPropertyAssign> getOptionalPropertiesAssignments(){return oAssignSet;};
    public OntRole getRole(){ return role;}

    public Set<String> getMandatoryColumnNames(){
        return mAssignSet.stream().map(OntPropertyAssign::getColumnName).collect(Collectors.toSet());
    }

    public Set<String> getColumnNames(){
        Set<String>  columnNames =  mAssignSet.stream().map(OntPropertyAssign::getColumnName).collect(Collectors.toSet());
        columnNames.addAll(oAssignSet.stream().map(OntPropertyAssign::getColumnName).collect(Collectors.toSet()));
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
        this.mAssignSet = assignedProperties.stream().filter(x -> entity.getMandatoryProperties().contains(x.getType())).collect(Collectors.toSet());
        this.oAssignSet = assignedProperties.stream().filter(x -> entity.getOptionalProperties().contains(x.getType())).collect(Collectors.toSet());
        this.role = role;
    }
}
