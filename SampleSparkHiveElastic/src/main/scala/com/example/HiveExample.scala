package com.example

import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types._

object HiveExample {
  def main(args: Array[String]): Unit = {
    // Spark session with Hive support
    val spark = SparkSession.builder()
      .appName("HiveExample")
      .enableHiveSupport()
      .getOrCreate()

    import spark.implicits._

    // Create a database
    spark.sql("CREATE DATABASE IF NOT EXISTS mydb")
    spark.sql("USE mydb")

    // Create a table
    spark.sql("""
      CREATE TABLE IF NOT EXISTS people (
        id INT,
        name STRING,
        age INT
      )
      STORED AS PARQUET
    """)

    // Insert some sample rows
    val data = Seq(
      (1, "Alice", 30),
      (2, "Bob", 25),
      (3, "Charlie", 28)
    )

    val df = data.toDF("id", "name", "age")
    df.write.mode("append").insertInto("people")

    // Query the table
    val result = spark.sql("SELECT * FROM people")
    result.show()

    spark.stop()

  }
}