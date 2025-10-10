package org.example.ontology;

import org.apache.spark.sql.Column;


public  class OntPropertyAssign {
    private final String columnName;
    private final OntProperty type;
    public OntPropertyAssign(OntProperty type, String columnName){
        this.columnName = columnName;
        this.type = type;
    }

    public String getColumnName() {
        return columnName;
    }

    public OntProperty getType() {
        return type;
    }
}
