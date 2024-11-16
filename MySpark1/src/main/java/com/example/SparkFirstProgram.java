package com.example;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;

public class SparkFirstProgram {

    void run() {
        final SparkSession spark = SparkSession.builder()
                .appName("SparkFirstProgram")
                .master("local[*]")
                .config("spark.es.index.auto.create", "true")
                .getOrCreate();

        //String csvFilePath = "C:\\Users\\a1234\\projects\\MySpark1\\src\\test\\data\\input.csv";
        String csvFilePath = "/mnt/c/Users/a1234/projects/MySpark1/src/test/data/input.csv";

        Dataset<Row> csvData = spark.read()
                .option("header", "true") // Set to "false" if no header in CSV
                .option("inferSchema", "true") // Automatically infers the schema
                .csv(csvFilePath);

        // Show the data (for verification)
        csvData.show();

        // Print the schema of the Dataset
        csvData.printSchema();

        FlatMapFunction<String, String> myFlatMap =  new SampleFlatMapRowToJson();
        Dataset<String> jsonDataset = csvData.toJSON();
        jsonDataset = jsonDataset.flatMap(myFlatMap, Encoders.STRING());
        jsonDataset.show(false);

        Dataset<Row> parsedDataset = spark.read().json(jsonDataset);
        parsedDataset.printSchema();
        parsedDataset.show();


        parsedDataset.write()
                .format("org.elasticsearch.spark.sql")
                .mode(SaveMode.Ignore)
                .option("es.nodes", "http://192.168.56.1")
                .option("es.port", "9200")
                .option("es.resource", "my_index10")
                .option("es.nodes.wan.only", "true")
                .option("es.http.timeout", "10s")
                .option("es.mapping.id", "id")
                .option("es.batch.size.entries", "100") // Batch size to control write performance
                .option("es.write.operation", "upsert")  // Optional: Upsert operation to avoid overwriting
                .option("es.retry.on.conflict", "1")    // Retry on conflict
                .save();



    }
    public static void main(final String[] args) {
           new SparkFirstProgram().run();
        }
    }
