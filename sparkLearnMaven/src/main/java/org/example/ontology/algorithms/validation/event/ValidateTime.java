package org.example.ontology.algorithms.validation.event;


import org.apache.spark.api.java.function.Function2;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class ValidateTime implements Function2<Dataset<Row>, Column,Dataset<Row>> {
    @Override
    public Dataset<Row> call(Dataset<Row> rowDataset, Column column) throws Exception {
        return rowDataset.where(column.isNotNull());
    }
}
