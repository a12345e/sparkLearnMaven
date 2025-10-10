package org.example.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.ontology.OntEventAssign;

public interface SourceI {
    public Dataset<Row> preProcess(Dataset<Row> data);
    public SourceManagement getSourceManagement();
    public OntEventAssign getEvent();
}
