package com.mat.spark.SparkWord

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
/**
 * author: jkc
 */
object SimpleApp {
    def main(args: Array[String]) {
     // eclipse 调试只需修改16行
    //本地单机模式下spark submit的运行指令       spark-submit   --class "com.mat.spark.SparkWord.SimpleApp" --master local[2] "E:\SparkWord-0.0.1-SNAPSHOT-jar-with-dependencies.jar" 
    //本地单机模式下文件存放位置 val logFile = "D:/bigdata/spark-1.6.2-bin-hadoop2.6/README.md" // Should be some file on your system
    //集群环境，需要先将文件上传到hdfs
    //集群环境下提交命令  spark-submit --master spark://master:7077 --class com.mat.spark.SparkWord.SimpleApp /home/hadoo/SparkWord-0.0.1-SNAPSHOT-jar-with-dependencies.jar
    val logFile = "/sparkword/README.md" // Should be some file on your system  // 这个可以作为一个输出参数传进来
    val conf = new SparkConf().setAppName("Simple Application")//.setMaster("local[*]")//在eclipse调试的时候，保留setMater， 运行maven build指令生成jar包的时候去掉  build 指令选择第二个，输入package
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