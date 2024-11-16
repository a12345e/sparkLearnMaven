package com.example;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import java.util.*;
import java.util.stream.Collectors;



public class Something {
    public static void main(String[] args) {
        // Create SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("RowToMapExample")
                .master("local")
                .getOrCreate();

        // Sample data with Row structure
        List<Row> rowData = Arrays.asList(
                RowFactory.create("John", "Doe", 30),
                RowFactory.create("Alice", "Smith", 28),
                RowFactory.create("Bob", "Brown", 35)
        );

        // Define schema for the DataFrame
        StructType schema = new StructType(new StructField[]{
                new StructField("first_name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("last_name", DataTypes.StringType, false, Metadata.empty()),
                new StructField("age", DataTypes.IntegerType, false, Metadata.empty())
        });

        // Create DataFrame from Row data and schema
        Dataset<Row> df = spark.createDataFrame(rowData, schema);

        // Show the original DataFrame
        df.show();

        // Convert the Dataset<Row> to Dataset<Map<String, String>>
        JavaRDD<Map<String, String>> mapRDD = df.javaRDD().map(row -> {
            // Convert each Row to a Map<String, String>
            Map<String, String> rowMap = new HashMap<>();
            for (String columnName : df.columns()) {
                rowMap.put(columnName, row.getAs(columnName).toString());
            }
            return rowMap;
        });

        // Convert JavaRDD<Map<String, String>> to Dataset<Map<String, String>>
//        Dataset<Map<String, String>> mapDataset = spark.createDataset(mapRDD.rdd(), Encoders.bean(Map.class));
//
//        // Show the resulting Dataset of Map<String, String>
//        mapDataset.show(false);
    }
}