package org.example.ontology.alg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.example.ontology.OntPropertyAssign;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.col;

public class StringNormalizer implements NormalizeProperty {

    @Override
    public Dataset<Row> normalize(Dataset<Row> data, OntPropertyAssign propertyAssign) {
        String name = propertyAssign.getColumnName();
        StructField field = data.schema().apply(name);
        if(field.dataType().equals(DataTypes.StringType)){
            return data.withColumn(name,when(col(name).equalTo(""), lit(null)).otherwise(col(name)));
        }else {
            return data;
        }
    }
}
