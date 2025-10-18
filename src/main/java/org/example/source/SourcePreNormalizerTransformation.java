package org.example.source;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class SourcePreNormalizerTransformation {
    Dataset<Row> transform(Dataset<Row> data){
        return data;
    }
}
