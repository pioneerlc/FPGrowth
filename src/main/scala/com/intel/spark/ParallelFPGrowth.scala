package com.intel.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * ParallelFPGrowth.scala
 * This is a navie implementation of Parallel FPGrowth for learning how to use Spark and Scala.
 * Author: Mark Lin
 * E-mail: chlin.ecnu@gmail.com
 * Version: 2.0
 */

object ParallelFPGrowth {
  def apply(input: RDD[String], supportThreshold: Int, numPerGroup: Int): Unit ={
    val transactions = input.map(line => line.split(" "))
    val fList= input.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).filter(_._2 >= supportThreshold).map(pair => pair.swap).sortByKey(false).map(pair => pair.swap).collect
    println("fList = ")
    for(i <- fList){
      println("item = " + i._1 + "    support = " + i._2)
    }
    val res = transactions.flatMap(line => ParallelFPGrowthMapper().map(line, fList, numPerGroup)).collect

    for(i <- res){
      if(i._2.nonEmpty){
        println("groupID = " + i._1 + " :")
        for(j <- i._2){
          print("itemID = " + j + " ")
        }
        println("\n")
      }
    }
  }
}

object ParallelFPGrowthMapper{
  def apply(): ParallelFPGrowthMapper ={
    val mapper = new ParallelFPGrowthMapper()
    mapper
  }
}

class ParallelFPGrowthMapper(val NUM_PER_GROUPS_DEFAULT: Int = 5){
  def map(transaction: Array[String], fList: Array[(String, Int)], numPerGroup: Int  = NUM_PER_GROUPS_DEFAULT): ArrayBuffer[(Int, ArrayBuffer[Int])] ={
    def getfMap(fList: Array[(String, Int)]): HashMap[String, Int] = {
      var i = 0
      val fMap = new HashMap[String, Int]()
      for(pair <- fList){
        fMap.put(pair._1, i)
        i += 1
      }
      fMap
    } //end of getfMap

    def getGroupID(item: Int, numPerGroup: Int): Int ={
      item / numPerGroup
    } //end of getGroupID

    var retVal = new ArrayBuffer[(Int, ArrayBuffer[Int])]()
    var itemArr = new ArrayBuffer[Int]()
    val fMap: HashMap[String, Int] = getfMap(fList)
    for(item <- transaction){
      if(fMap.keySet.contains(item)){
        itemArr += fMap(item)
      }
    }
    itemArr = itemArr.sortWith(_ < _)
    val groups = new ArrayBuffer[Int]()
    for(i <- (0 until itemArr.length).reverse){
      val item = itemArr(i)
      val groupID = getGroupID(item, numPerGroup)
      if(!groups.contains(groupID)){
        val tempItems = new ArrayBuffer[Int]()
        tempItems ++= itemArr.slice(0, i + 1)
        retVal += groupID -> tempItems
        groups += groupID
      }
    }
    retVal
  } //end of map
} //end of ParallelFPGrowthMapper



