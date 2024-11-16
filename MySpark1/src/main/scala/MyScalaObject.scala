  import org.apache.spark.sql.{DataFrame, SparkSession}
  import org.apache.spark.sql.functions._

  object SparkScalaApp {
    def main(args: Array[String]): Unit = {
      // Create a SparkSession
      val spark = SparkSession.builder()
        .appName("Simple Spark App")
        .getOrCreate()

      // Sample data
      val data = Seq(
        (1, "Alice", 25),
        (2, "Bob", 30),
        (3, "Charlie", 22)
      )

      // Define the schema for the DataFrame
      val schema = List("id", "name", "age")

      // Create a DataFrame from the sample data and schema
      val df: DataFrame = spark.createDataFrame(data).toDF(schema: _*)

      // Perform a simple transformation (add 5 to the age column)
      val transformedDF = df.withColumn("age_plus_5", col("age") + 5)

      // Stop the SparkSession
      spark.stop()
    }
  }

