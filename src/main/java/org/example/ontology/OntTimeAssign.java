package org.example.ontology;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class OntTimeAssign {
    private final OntTime type;
    private final Collection<OntPropertyAssign> mAssignSet;
    private final OntTime getType() { return type;};
    private final Collection<OntPropertyAssign> getMandatoryPropertiesAssignments() { return mAssignSet;};
    public OntTimeAssign(OntTime type, Collection<OntPropertyAssign> assignedProperties){
        Set<OntProperty> properties = assignedProperties.stream().map(OntPropertyAssign::getType).collect(Collectors.toSet());
        if(!properties.containsAll(type.getMandatoryProperties())){
            throw new RuntimeException("Missing mandatory properties in provided properties:: " +properties);
        }
        properties.removeAll(new HashSet<>(type.getMandatoryProperties()));
        if(!properties.isEmpty()){
            throw new RuntimeException("Defined invalid properties for this entity "+properties);
        }
        this.type = type;
        this.mAssignSet = assignedProperties.stream().filter(x -> type.getMandatoryProperties().contains(x.getType())).collect(Collectors.toSet());
    }
}
