package org.example.spark;

import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.BeforeClass;

public class SetupTest {
    protected static SparkSession spark;

    @BeforeClass
    public static void setup() {
        spark = SparkSession.builder()
                .master("local[*]")
                .appName("TestApp")
                .config("spark.ui.enabled", "false")
                .getOrCreate();
    }
    @AfterClass
    public static void tearDown(){
        spark.close();
    }
}
