package org.example.ontology;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public enum OntTime {
    range(new HashSet<>(Arrays.asList(OntProperty.start_time, OntProperty.end_time))),
    point(new HashSet<>(Collections.singletonList(OntProperty.time))),
    always(new HashSet<>());

    final Set<OntProperty> mpSet;
    public Set<OntProperty> getMandatoryProperties(){
        return mpSet;
    };
    OntTime(Set<OntProperty> mpSet){
        this.mpSet=mpSet;
    }
}
