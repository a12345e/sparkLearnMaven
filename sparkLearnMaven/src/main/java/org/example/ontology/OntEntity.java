package org.example.ontology;

import org.glassfish.jersey.internal.guava.Sets;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public enum OntEntity {

    airplane(new HashSet<>(Arrays.asList(OntProperty.wing, OntProperty.tail, OntProperty.color)),new HashSet<>(Arrays.asList(OntProperty.manufacturer)));
    final Set<OntProperty> mpSet;
    final Set<OntProperty> opSet;
    public Set<OntProperty> getMandatoryProperties(){
        return mpSet;
    };
    public Set<OntProperty> getOptionalProperties(){
        return opSet;
    };
    OntEntity(Set<OntProperty> mpSet,Set<OntProperty> opSet ){
        this.mpSet=mpSet;
        this.opSet=opSet;
    }
}
