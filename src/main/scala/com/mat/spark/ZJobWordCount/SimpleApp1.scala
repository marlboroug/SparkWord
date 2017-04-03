package com.mat.spark.ZJobWordCount

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * author: jkc
 */
object SimpleApp1 {
   def main(args: Array[String]) {
    val logFile = "D:/bigdata/spark-1.6.2-bin-hadoop2.6/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("**********************************************")
    println(s"Lines with a: $numAs, Lines with b: $numBs")
    println("**********************************************")
    sc.stop()
   }
}