package com.intel.spark

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer

/**
 * FPGrowth.scala
 * This is a navie implementation of FPGrowth|Parallel FPGrowth for learning how to use Spark and Scala.
 * Author: Mark Lin
 * E-mail: chlin.ecnu@gmail.com
 * Version: 2.1
 */

object FPGrowth {
  def showWarning(): Unit = {
    System.err.println(
      """
        |WARN:
        |This is a navie implementation of FPGrowth|Parallel FPGrowth for learning how to use Spark and Scala.
        |Author: Mark Lin
        |E-mail: chlin.ecnu@gmail.com
        |Version: 2.1
      """.stripMargin)
  }

  def showError(): Unit = {
    println("Usage: java -jar code.jar dependencies.jar supportThreshold method<sequential|parallel> numPerGroup(Optional)")
    System.exit(-1)
  }
  def main(args: Array[String]): Unit = {
    if(args.length + 1 > 5 || args.length + 1 < 4){
      showError()
    }
    showWarning()

    val jars = ListBuffer[String]()
    args(0).split(",").map(jars += _)
    val supportThreshold = args(1).toInt
    val method = if(args(2).equals("sequential")) 0 else if(args(2).equals("parallel")) 1 else showError()
    val NUM_PER_GROUPS_DEFAULT = 5
    var numPerGroup = NUM_PER_GROUPS_DEFAULT
    if((args.length + 1) == 5){
      numPerGroup = args(3).toInt
      if(numPerGroup > 5){
        println("WARN:")
        println("Too large numPerGroup may lead to wrong answer.")
      }
    }

    val conf = new SparkConf()
    conf.setMaster("spark://localhost:7077").setAppName("FPGrowth").set("spark.executor.memory", "64m").setJars(jars)
    val sc = new SparkContext(conf)
    val input = sc.textFile("hdfs://localhost:9000/hduser/wordcount/input")


    if(method == 0){
      val transactions = input.map(line => line.split(" ")).collect() //transform RDD to Array[Array[String]]
      val fptree = FPTree(transactions, supportThreshold)
      sc.makeRDD(fptree.patterns).saveAsTextFile("hdfs://localhost:9000/hduser/wordcount/output/")
    }else{
      ParallelFPGrowth(input, supportThreshold, numPerGroup)
    }

    sc.stop()
  }
}

