package com.example;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import scala.Function1;
import scala.Tuple2;

import java.util.*;

import org.apache.spark.api.java.function.FlatMapFunction;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;


public class SparkFirstProgram {


    void run() {
        final SparkSession spark = SparkSession.builder()
                .appName("SparkFirstProgram")
                .config("spark.jars.packages", "org.elasticsearch:elasticsearch-spark-30_2.12:7.13.1")
                .master("local[*]")
                .config("spark.es.index.auto.create", "true")
                .getOrCreate();

        String csvFilePath = "C:\\Users\\a1234\\projects\\MySpark1\\src\\test\\data\\input.csv";
        //String csvFilePath = "/mnt/c/Users/a1234/projects/MySpark1/src/test/data/input.csv";

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
                .option("es.nodes", "192.168.56.1")
                .option("es.port", "9200")
                .option("es.resource", "index/_doc")
                .option("es.nodes.wan.only", "true")
                .option("es.http.timeout", "10s")
                .option("es.mapping.id", "id")
                .option("es.mapping.index", "index")
                .option("es.batch.size.entries", "100") // Batch size to control write performance
                .option("es.write.operation", "upsert")  // Optional: Upsert operation to avoid overwriting
                .option("es.retry.on.conflict", "1")
                .option("es.update.script", "ctx._source.counter1 = Math.max(ctx._source.counter1, 50)")
                .option("es.update.script.params", "value:40")// Retry on conflict
                .save();



    }
    public static void main(final String[] args) {
           new SparkFirstProgram().run();
        }
    }
