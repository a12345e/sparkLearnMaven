package org.example.ontology;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface Normalize {
    public Dataset<Row> normalize(Dataset<Row> data);
}
