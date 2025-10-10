package org.example.ontology.alg;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Row;

import java.util.Set;

public class AnyNullColAllNull {
    public Dataset<Row>transform(Dataset<Row> data, Set<String> cols){
        Column anyNull = lit(false);
        for (String c : cols) {
            anyNull = anyNull.or(col(c).isNull());
        }
        for (String c : cols) {
            data = data.withColumn(c, when(anyNull, lit(null)).otherwise(col(c)));
        }
        return data;
    }
}
