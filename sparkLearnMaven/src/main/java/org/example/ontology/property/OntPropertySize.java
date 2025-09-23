package org.example.ontology.property;

import org.apache.spark.sql.Column;
import org.example.ontology.OntProperty;
import org.example.ontology.OntPropertyType;

public class OntPropertySize extends OntProperty  {
    @Override
    public OntPropertyType getType() {
        return OntPropertyType.size;
    }
    public OntPropertySize(Column col)
    {
     super(col);
    }

}
