package org.example.ontology;

import org.apache.spark.sql.Column;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;


public abstract class OntProperty implements Normalize{
    private final Column column;
    public OntProperty(Column column){
        this.column = column;
    }
    @Override
    public Dataset<Row> normalize(Dataset<Row> data) {
        String name = column.expr().toString();
        StructField field = data.schema().apply(name);
        if(field.dataType().equals(DataTypes.StringType)){
            return data.withColumn(name,when(column.equalTo(""), lit(null)).otherwise(column));
        }else {
            return data;
        }

    }
    public abstract OntPropertyType getType();

}
