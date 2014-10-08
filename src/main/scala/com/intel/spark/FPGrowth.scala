package com.intel.spark

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import org.apache.spark.SparkContext._

/**
 * FPGrowth.scala
 * This is a navie implementation of FPGrowth for learning how to use Spark and Scala.
 * Author: Mark Lin
 * E-mail: chlin.ecnu@gmail.com
 * Version: 1.1
 */


object FPGrowth {
  def showWarning(): Unit ={
    System.err.println(
      """
        |WARN:
        |This is a navie implementation of FPGrowth for learning how to use Spark and Scala.
        |Author: Mark Lin
        |E-mail: chlin.ecnu@gmail.com
        |Version: 1.1
      """.stripMargin)
  }
  def main(args: Array[String]): Unit = {
    if(args.length + 1 < 3){
      println("Usage: java -jar code.jar dependencies.jar supportThreshold")
      System.exit(-1)
    }

    val jars = ListBuffer[String]()
    args(0).split(",").map(jars += _)
    val supportThreshold = args(1).toInt

    showWarning()

    val conf = new SparkConf()
    conf.setMaster("spark://localhost:7077").setAppName("FPGrowth").set("spark.executor.memory", "64m").setJars(jars)
    val sc = new SparkContext(conf)
    val input = sc.textFile("hdfs://localhost:9000/hduser/wordcount/input")
    val records = input.map(line => line.split(" ")).collect() // transform RDD to Array[Array[String]]
    val fptree = FPTree(records, supportThreshold)
    sc.makeRDD(fptree.patterns).saveAsTextFile("hdfs://localhost:9000/hduser/wordcount/output/")

    //val check = sc.textFile("hdfs://localhost:9000/hduser/wordcount/output/")
    //val results = check.map(line => Array(line)).collect()
    //for(result <- results){
      //println("start")
      //for(item <- result){
        //println(item)
      //}
      //println("end")
    //}
    sc.stop()
  }
}

