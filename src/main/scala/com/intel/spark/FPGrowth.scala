package com.intel.spark

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer

/**
 * FPGrowth.scala
 * This is a naive implementation of FPGrowth|Parallel FPGrowth for learning how to use Spark and Scala.
 * Author: Mark Lin
 * E-mail: chlin.ecnu@gmail.com
 * Version: 2.0
 */

object FPGrowth {
  def showWarning(): Unit = {
    System.err.println(
      """
        |WARN:
        |This is a navie implementation of FPGrowth|Parallel FPGrowth for learning how to use Spark and Scala.
        |Author: Mark Lin
        |E-mail: chlin.ecnu@gmail.com
        |Version: 2.0
      """.stripMargin)
  }

  def showError(): Unit = {
    println(
      """
        |Usage: java -jar code.jar dependencies.jar minSupport method<sequential|parallel> numGroups
        |Parameters:
        |  minSupport - The minimum number of times a co-occurrence must be present
        |  method - Method of processing: sequential|parallel
        |  numGroups - (Optional) Number of groups the features should be divided in the parallel version
      """.stripMargin)
    System.exit(-1)
  }
  def main(args: Array[String]): Unit = {
    if(args.length + 1 > 5 || args.length + 1 < 4){
      showError()
    }
    showWarning()

    val jars = ListBuffer[String]()
    args(0).split(",").map(jars += _)
    val minSupport = args(1).toInt
    val method = if(args(2).equals("sequential")) 0 else if(args(2).equals("parallel")) 1 else showError()

    val conf = new SparkConf()
    conf.setMaster("spark://localhost:7077").setAppName("FPGrowth").set("spark.executor.memory", "128m").setJars(jars)
    val sc = new SparkContext(conf)
    val input = sc.textFile("hdfs://localhost:9000/hduser/wordcount/input/input.csv", 3)

    if(method == 0) {
      val transactions = input.map(line => line.split(",")).collect() //transform RDD to Array[Array[String]]
      val fptree = FPTree(transactions, minSupport)
      sc.makeRDD(fptree.patterns).saveAsTextFile("hdfs://localhost:9000/hduser/fpgrowth/output/")
    } else {
      val NUM_GROUPS_DEFAULT = 5
      var numGroups = NUM_GROUPS_DEFAULT
      if((args.length + 1) == 5){
        numGroups = args(3).toInt
        if(numGroups > 5){
          println("WARN:")
          println("Too large numGroups might lead to wrong answer.")
        }
      }

      val patterns = ParallelFPGrowth(input, minSupport, numGroups)
      patterns.saveAsTextFile("hdfs://localhost:9000/hduser/fpgrowth/output/")
    }

    sc.stop()
  }
}

