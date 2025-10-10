package com.example

import org.apache.spark.sql.api.java.UDF1


object CommonUdfsScala {
  val toUpper: UDF1[String, String] =
    (s: String) => if (s == null) null else s.toUpperCase

  val duplicate: UDF1[String, String] =
    (s: String) => if (s == null) null else s + s
}