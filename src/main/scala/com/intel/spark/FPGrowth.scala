package com.intel.spark

import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer

/**
 * File: FPGrowth.scala
 * Description: This is a naive implementation of FPGrowth | Parallel FPGrowth for learning how to use Spark and Scala.
 * Author: Lin, Chen
 * E-mail: chlin.ecnu@gmail.com
 * Version: 2.3
 */

object FPGrowth {
  def showWarning() {
    System.err.println(
      """
        |---------------------------------------------------------------------
        |WARN:
        |Description: This is a naive implementation of FPGrowth|Parallel FPGrowth for learning how to use Spark and Scala.
        |Author: Lin, Chen
        |E-mail: chlin.ecnu@gmail.com
        |Version: 2.3
        |---------------------------------------------------------------------
      """.stripMargin)
  }

  def showError(): Unit = {
    println(
      """
        |Usage: java -jar code.jar dependencies.jar minSupport method<sequential|parallel> numGroups
        |Parameters:
        |  minSupport - The minimum number of times a co-occurrence must be present
        |  method - Method of processing: sequential or parallel
        |  numGroups - (Optional) Number of groups the features should be divided in the parallel version
      """.stripMargin)
    System.exit(-1)
  }
  def main(args: Array[String]): Unit = {

    if(args.length + 1 > 5 || args.length + 1 < 4){
      showError()
    }

    showWarning()

    //Receive parameters from console.
    val jars = ListBuffer[String]()
    args(0).split(",").map(jars += _)
    val minSupport = args(1).toInt
    val method = if(args(2).equals("sequential")) 0 else if(args(2).equals("parallel")) 1 else showError()

    //Initialize SparkConf.
    val conf = new SparkConf()
    conf.setMaster("spark://10.1.2.71:7077").setAppName("FPGrowth").set("spark.executor.memory", "512m").setJars(jars)

    //Initialize SparkContext.
    val sc = new SparkContext(conf)

    //Create distributed datasets from hdfs.
    val input = sc.textFile("hdfs://10.1.2.71:54310/user/clin/fpgrowth/input/input.txt", 4)

    //Run FPGrowth.
    if(method == 0) {
      //Transform RDD to Array[Array[String]].
      val transactions = input.map(line => line.split(",")).collect()

      //Initialize FPTree and start to  mine frequent patterns from FPTree.
      val patterns = FPTree(transactions, minSupport)

      //Write the elements of patterns as text files in a given directory in hdfs.
      sc.makeRDD(patterns).saveAsTextFile("hdfs://10.1.2.71:54310/user/clin/fpgrowth/output/")
    }
    //Run Parallel FPGrowth.
    else {
      //Set the default number of groups.
      val NUM_GROUPS_DEFAULT = 5
      var numGroups = NUM_GROUPS_DEFAULT

      //If user specifies the number of groups, numGroups is overridden.
      if((args.length + 1) == 5){
        numGroups = args(3).toInt
      }

      //Initialize ParallelFPGrowth and start to mine frequent patters in parallel.
      val patterns = ParallelFPGrowth(input, minSupport, numGroups)

      //Write elements of patterns as text files in a given directory in hdfs.
      patterns.saveAsTextFile("hdfs://10.1.2.71:54310/user/clin/fpgrowth/output/")
    }

    //Stop SparkContext.
    sc.stop()
  }
}

