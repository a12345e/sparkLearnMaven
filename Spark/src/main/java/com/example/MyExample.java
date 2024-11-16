package com.example;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import scala.Function1;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;


public class MyExample
{
// External FlatMap function class
static class RowToJsonFlatMap implements FlatMapFunction<Row, String> {
    @Override
    public Iterator<String> call(Row row) {
        List<String> jsonStrings = new ArrayList<>();
        String baseJson = "{\"id\": " + row.getAs("id") + ", \"name\": \"" + row.getAs("name") + "\"}";

        // Adding variations of JSON strings for each row
        jsonStrings.add(baseJson);
        jsonStrings.add("{\"id\": " + row.getAs("id") + ", \"name\": \"" + row.getAs("name") + "\", \"type\": \"extended\"}");

        return jsonStrings.iterator();
    }
}

public static class DatasetFlatMapToElasticsearchWithAuth {

    public static void main(String[] args) {
//        SparkSession spark = SparkSession.builder()
//                .appName("FlatMap Dataset to Elasticsearch with Auth Example")
//                .master("172.17.152.175:87077")  // For local testing
//                .config("es.nodes", "localhost")  // Elasticsearch node
//                .config("es.port", "9200")         // Elasticsearch port
//                .config("es.index.auto.create", "true")  // Automatically create index if it doesn't exist
//                .config("es.net.http.auth.user", "your_username")  // Username for Elasticsearch
//                .config("es.net.http.auth.pass", "your_password")  // Password for Elasticsearch
//                .config("es.nodes.wan.only", "true")  // Required for some remote connections
//                .getOrCreate();

        // Define schema for the DataFrame
//        StructType schema = new StructType(new StructField[]{
//                new StructField("id", DataTypes.IntegerType, false, Metadata.empty()),
//                new StructField("name", DataTypes.StringType, false, Metadata.empty())
//        });
//
//        // Create sample data
//        Row row1 = RowFactory.create(1, "Alice");
//        Row row2 = RowFactory.create(2, "Bob");
//        Dataset<Row> df = spark.createDataFrame(Collections.singletonList(row1), schema)
//                .union(spark.createDataFrame(Collections.singletonList(row2), schema));
//
//        // Use the external FlatMap function
//        Dataset<String> jsonDataset = df.flatMap(new RowToJsonFlatMap(), Encoders.STRING());
//
//        Dataset<Row> jsonRowDataset = jsonDataset.map((Function1<String, Row>) RowFactory::create,
//                RowEncoder.apply(new StructType().add("json", DataTypes.StringType))
//        );
//
//        // Write Dataset<Row> to Elasticsearch
//        JavaEsSparkSQL.saveToEs(jsonRowDataset, "my_index/_doc", Collections.singletonMap("es.mapping.id", "id"));
//
       // spark.stop();
    }
}}


