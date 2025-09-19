package org.example.ontology;

public class OntRelation {
    private final OntObject source;
    private final OntObject destination;
    private final OntRelationType type;

    public OntObject getSource() { return source;};
    public OntObject getDestination(){ return destination;};
    public OntRelationType getType(){ return type;};

    public OntRelation(OntObject source,OntObject destination,OntRelationType type){
        this.source = source;
        this.destination  = destination;
        this.type = type;
    }
}
