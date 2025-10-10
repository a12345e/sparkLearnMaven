package com.example;

import org.apache.spark.sql.api.java.UDF1;

public class CommonUdfs {
    public static class ReverseString implements UDF1<String, String> {
        @Override
        public String call(String s) {
            if (s == null) return null;
            return new StringBuilder(s).reverse().toString();
        }
    }

    public static class StringLength implements UDF1<String, Integer> {
        @Override
        public Integer call(String s) {
            return (s == null) ? null : s.length();
        }
    }
}