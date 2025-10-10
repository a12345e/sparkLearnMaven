package org.example.spark;



import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Encoders;
import java.util.Arrays;
import java.util.List;

public class MySparkApp {
    public void run(SparkSession spark){
        List<Person> people = Arrays.asList(
                new Person("Alice", 30),
                new Person("Bob", 25),
                new Person("Charlie", 35)
        );

        // 3. Create Dataset from Java objects
        Dataset<Person> ds = spark.createDataset(people, Encoders.bean(Person.class));

        // 4. Convert to DataFrame (Dataset<Row>)
        Dataset<Row> df = ds.toDF();

        // 5. Write as CSV (overwrite if exists)
        df.coalesce(1).write()
                .option("header", "true")   // include column names
                .mode("overwrite")
                .csv("output/people_csv");

        df.coalesce(1).write()
                .mode("overwrite")
                .parquet("output/people_parquet");

        Dataset<Row> dr = spark.read().parquet("output/people_parquet");
        dr.show();
    }

    // Simple Java bean class
    public static class Person {
        private String name;
        private int age;

        // Required: public no-arg constructor
        public Person() {}

        public Person(String name, int age) {
            this.name = name;
            this.age = age;
        }

        // Getters and setters (needed for Encoders.bean)
        public String getName() { return name; }
        public void setName(String name) { this.name = name; }

        public int getAge() { return age; }
        public void setAge(int age) { this.age = age; }
    }



    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().appName("xx").master("local[*]").getOrCreate();
        new MySparkApp().run(spark);
        spark.stop();

    }
}
