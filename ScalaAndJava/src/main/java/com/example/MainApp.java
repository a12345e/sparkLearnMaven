package com.example;

public class MainApp {
    public static void main(String[] args) {
        System.out.println("Calling Scala Spark job from Java...");
        System.out.println("Java runtime = " + System.getProperty("java.runtime.version"));
        System.out.println("Java VM name = " + System.getProperty("java.vm.name"));
        System.out.println("Java version = " + System.getProperty("java.version"));
        ScalaSparkJob.runSparkJob(); // Call Scala object method
    }
}