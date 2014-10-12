package com.intel.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, HashMap, Set}

/**
 * ParallelFPGrowth.scala
 * This is a navie implementation of Parallel FPGrowth for learning how to use Spark and Scala.
 * Author: Mark Lin
 * E-mail: chlin.ecnu@gmail.com
 * Version: 2.1
 */

object ParallelFPGrowth {
  def apply(input: RDD[String], supportThreshold: Int, numPerGroup: Int): Unit ={
    val transactions = input.map(line => line.split(" ")).cache
    val fList= input.flatMap(line => line.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).filter(_._2 >= supportThreshold).map(pair => pair.swap).sortByKey(false).map(pair => pair.swap).cache.collect

    val patterns = transactions.flatMap(line => ParallelFPGrowthMapper().map(line, fList, numPerGroup)).groupByKey.flatMap(line => ParallelFPGrowthReducer().reduce(line, supportThreshold)).cache.collect
    for(pattern <- patterns.distinct){
      print(pattern)
    }
  }
}

object ParallelFPGrowthMapper{
  def apply(): ParallelFPGrowthMapper ={
    val mapper = new ParallelFPGrowthMapper()
    mapper
  }
}

class ParallelFPGrowthMapper(){
  def map(transaction: Array[String], fList: Array[(String, Int)], numPerGroup: Int): ArrayBuffer[(Int, ArrayBuffer[String])] ={
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

    var retVal = new ArrayBuffer[(Int, ArrayBuffer[String])]()
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
        val items = tempItems.map(x => fMap.map(_.swap).getOrElse(x, null))
        retVal += groupID -> items
        groups += groupID
      }
    }
    retVal
  } //end of map
} //end of ParallelFPGrowthMapper

object ParallelFPGrowthReducer{
  def apply(): ParallelFPGrowthReducer ={
    val reducer = new ParallelFPGrowthReducer()
    reducer
  }
}

class ParallelFPGrowthReducer(){
  def reduce(line: (Int, Iterable[ArrayBuffer[String]]), supportThreshold: Int): ArrayBuffer[String] ={
    val transactions = line._2
    val localFPTree = new LocalFPTree(new ArrayBuffer[String]())
    localFPTree.FPGrowth(transactions, new ArrayBuffer[String], supportThreshold)
    localFPTree.patterns
  }
}

class LocalFPTree(var patterns: ArrayBuffer[String]){

  def buildHeaderTable(transactions: Iterable[ArrayBuffer[String]], supportThreshold: Int): ArrayBuffer[TreeNode] = {
    if(transactions.nonEmpty){
      val map: HashMap[String, TreeNode] = new HashMap[String, TreeNode]()
      for(transaction <- transactions){
        for(name <- transaction){
          if(!map.contains(name)){
            val node: TreeNode = new TreeNode(name)
            node.count = 1
            map(name) = node
          }else{
            map(name).count += 1
          }
        }
      }
      val headerTable = new ArrayBuffer[TreeNode]()
      map.filter(_._2.count >= supportThreshold).values.toArray.sortWith(_.count > _.count).copyToBuffer(headerTable)
      headerTable //return headerTable
    }else{
      null //if transactions is empty, return null
    }
  } //end of buildHeaderTable

  def buildLocalFPTree(transactions: Iterable[ArrayBuffer[String]], headerTable: ArrayBuffer[TreeNode]): TreeNode = {
    val root: TreeNode = new TreeNode()
    for(transaction <- transactions){
      val sortedTransaction = sortByHeaderTable(transaction, headerTable)
      var subTreeRoot: TreeNode = root
      var tmpRoot: TreeNode = null
      if(root.children.nonEmpty){
        while(sortedTransaction.nonEmpty && subTreeRoot.findChild(sortedTransaction.head) != null){
          tmpRoot = subTreeRoot.children.find(_.name.equals(sortedTransaction.head)) match {
            case Some(node) => node
            case None => null
          }
          tmpRoot.count += 1
          subTreeRoot = tmpRoot
          sortedTransaction.remove(0)
        } //end of while
      } //end of if
      addNodes(subTreeRoot, sortedTransaction, headerTable)
    } //end of for

    def sortByHeaderTable(transaction: ArrayBuffer[String], headerTable: ArrayBuffer[TreeNode]): ArrayBuffer[String] = {
      val map: HashMap[String, Int] = new HashMap[String, Int]()
      for(item <- transaction){
        for(index <- 0 until headerTable.length){
          if(headerTable(index).name.equals(item)){
            map(item) = index
          }
        }
      }

      val sortedTransaction: ArrayBuffer[String] = new ArrayBuffer[String]()
      map.toArray.sortWith(_._2 < _._2).foreach(sortedTransaction += _._1)
      sortedTransaction //return sortedTransaction
    } //end of sortByHeaderTable

    def addNodes(parent: TreeNode, transaction: ArrayBuffer[String], headerTable: ArrayBuffer[TreeNode]){
      while(transaction.nonEmpty){
        val name: String = transaction.head
        transaction.remove(0)
        val leaf: TreeNode = new TreeNode(name)
        leaf.count = 1
        leaf.parent = parent
        parent.children += leaf

        var cond = true //for breaking out of while loop
        var index: Int = 0
        while(cond && index < headerTable.length){
          var node = headerTable(index)
          if(node.name.equals(name)){
            while(node.nextHomonym != null)
              node = node.nextHomonym
            node.nextHomonym  = leaf
            cond = false
          }
          index += 1
        }

        addNodes(leaf, transaction, headerTable)
      }
    } //end of addNodes

    root //return root
  } //end of buildTransactionTree

  def FPGrowth(transactions: Iterable[ArrayBuffer[String]], postPattern: ArrayBuffer[String], supportThreshold: Int){
    val headerTable: ArrayBuffer[TreeNode] = buildHeaderTable(transactions, supportThreshold)

    val treeRoot = buildLocalFPTree(transactions, headerTable)

    if(treeRoot.children.nonEmpty){
      if(postPattern.nonEmpty){
        for(node <- headerTable){
          var result: String = ""
          result += "Frequency: " + node.count + " " + node.name +  " "
          for(pattern <- postPattern){
            result += pattern + " "
          }
          result += "\n"
          if(!patterns.contains(result))
            patterns += result
        }
      }

      for (node: TreeNode <- headerTable) {
        val newPostPattern: ArrayBuffer[String] = new ArrayBuffer[String]()
        newPostPattern += node.name
        if (postPattern.nonEmpty)
          newPostPattern ++= postPattern
        val newTransactions: ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]()
        var backNode: TreeNode = node.nextHomonym
        while (backNode != null) {
          var counter: Int = backNode.count
          val preNodes: ArrayBuffer[String] = new ArrayBuffer[String]()
          var parent: TreeNode = backNode.parent
          while (parent.name != null) {
            preNodes += parent.name
            parent = parent.parent
          }
          while (counter > 0) {
            newTransactions += preNodes
            counter -= 1
          }
          backNode = backNode.nextHomonym
        }

        FPGrowth(newTransactions, newPostPattern, supportThreshold)
      } //end of for

    } //end of if
  } //end of FPGrowth
} //end of LocalFPTree
