package com.example;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.net.URISyntaxException;
import java.sql.*;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.RowFactory;


public class App {


    public static void main(String[] args) throws URISyntaxException, SQLException {

        System.out.println(
                String.format("The current shell is: %s.", System.getenv("SHELL"))
        );
        System.out.println(System.getenv());


        SparkSession spark = SparkSession.builder()
                .appName("HDFS").master("local[2]")
                .getOrCreate();

        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        System.out.println(sc.appName());
        System.out.println(sc.sc().master());
        System.out.println(sc.sc().deployMode());
        System.out.println(sc.sc().getSparkHome());


        // Create a list of integers
        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // Parallelize the list to create an RDD
        JavaRDD<Integer> rdd = sc.parallelize(data);

        // Perform a map operation to square each number
        JavaRDD<Integer> squaredRdd = rdd.map(x -> {
            System.out.println("Processing " + x + " on thread: " + Thread.currentThread().getName());
            return x * x;
        });

        // Collect the results
        List<Integer> squaredData = squaredRdd.collect();

        // Print the results
        squaredData.forEach(System.out::println);


            List<Row> rows = Arrays.asList(
                RowFactory.create(1, "John Doe"),
                RowFactory.create(2, "Jane Doe")
            );

// Define the schema
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField("id", DataTypes.IntegerType, false),
                DataTypes.createStructField("name", DataTypes.StringType, false)
        });

// Create DataFrame
        Dataset<Row> df = spark.createDataFrame(rows, schema);
        df.createOrReplaceTempView("people_temp");
        df.write().option("mode","overwrite").saveAsTable("people");
        Dataset<Row> result = spark.sql("SELECT * FROM people");
        result.show();
        // Stop SparkSession



        String url = "jdbc:hive2://alon-ecker:10000/default";
        Connection conn = DriverManager.getConnection(url, "", "");
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT * FROM people");
        while (rs.next()) {
            System.out.println(rs.getString(1));
        }
        rs = stmt.executeQuery("drop table people");


        spark.stop();


    }
}