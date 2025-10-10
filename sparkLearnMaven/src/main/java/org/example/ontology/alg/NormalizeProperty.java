package org.example.ontology.alg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.ontology.OntPropertyAssign;

public interface NormalizeProperty {
    public Dataset<Row> normalize(Dataset<Row> data, OntPropertyAssign propertyAssign);
}
