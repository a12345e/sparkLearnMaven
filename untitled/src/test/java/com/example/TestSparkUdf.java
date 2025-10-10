package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.Test;
import com.example.CommonUdfs;
import com.example.CommonUdfsScala;

import java.util.Arrays;

import static com.example.CommonUdfsScala.*;

public class TestSparkUdf {
    @Test
    public void testMySpark(){
        SparkSession spark = SparkSession.builder()
                .appName("Java App using shared UDF JAR")
                .master("local[*]")
                .getOrCreate();

        // Register UDFs from the library
        spark.udf().register("reverseStr", new CommonUdfs.ReverseString(), DataTypes.StringType);
        spark.udf().register("strLen", new CommonUdfs.StringLength(), DataTypes.IntegerType);
        spark.udf().register("toUpper", CommonUdfsScala$.MODULE$.toUpper(), DataTypes.StringType);
        spark.udf().register("duplicate", CommonUdfsScala$.MODULE$.duplicate(), DataTypes.StringType);

        // Data
        StructType schema = new StructType().add("text", DataTypes.StringType);
        Dataset<Row> df = spark.createDataFrame(
                Arrays.asList(RowFactory.create("spark"), RowFactory.create("java+scala")),
                schema);

        df.createOrReplaceTempView("words");
        spark.sql("SELECT text, reverseStr(text), strLen(text), toUpper(text), duplicate(text) FROM words").show();

        spark.stop();
    }
}
