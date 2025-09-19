package com.example;


object ScalaSparkJob {
  case class Employee(name: String, dept: String, salary: Double)

  def runSparkJob(): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("ScalaSparkJob")
      .master("local[*]") // use all local cores
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame
    val df: DataFrame = Seq(
      (1, "Alice"),
      (2, "Bob"),
      (3, "Charlie")
    ).toDF("id", "name")

    df.show()

    val ds = Seq(
      Employee("Alice", "Engineering", 5000),
      Employee("Bob", "Sales", 4000)
    ).toDS()

    ds.map {
      case Employee(n, "Engineering", s) => s"$n works in Engineering and earns $s"
      case Employee(n, d, s) => s"$n works in $d"
    }.show()

    spark.stop()
  }
}