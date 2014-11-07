package com.intel.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import scala.collection.mutable.{ArrayBuffer, HashMap}

/**
 * File: ParallelFPGrowth.scala
 * Description: This is an implementation of Parallel FPGrowth as described in Haoyuan Li, et al.,
 * "PFP: Parallel FP-Growth for Query Recommendation", Proceedings of the 2008 ACM conference on recommender systems, 2008.
 * Author: Lin, Chen
 * E-mail: chlin.ecnu@gmail.com
 * Version: 2.3
 */

object ParallelFPGrowth {
  def apply(input: RDD[String], minSupport: Int, numGroups: Int): RDD[(String, Int)] ={
    /**
     * Split each line of transactions into items, map items of each line to Array[String] and we will obtain one RDD[Array[String]].
     * Cache this RDD in case it will be reused.
     */
    val transactions = input.map(line => line.split(",")).cache

    /**
     * Split each line of transactions into items, flatMap transformation will transform all items of all lines to one RDD[String].
     * Then we count the number of times every item appears in the transaction database and this is the so-called support value in the paper. For short, we just call it support.
     * After that, we will filter fList by minSupport and sort it by items' names in ascending order and suppports in descending order.
     * This RDD[(String, Int)] need to be cached and collected.
     */
    val fList= input.flatMap(line => line.split(",")).map(word => (word, 1)).reduceByKey(_ + _).filter(_._2 >= minSupport).sortByKey(true).map(pair => pair.swap).sortByKey(false).map(pair => pair.swap).cache.collect

    // For the convenience of dividing all items in fList to numGroups groups, we assign a unique ID which starts from 0 to (the length of fList - 1) to every item in fList.
    val fMap = getFMap(fList)

    // Compute the number of items in each group.
    val maxPerGroup = getMaxPerGroup(fList, numGroups)

    /**
      * ParallelFPGrowthMapper generates group-dependent transactions - one or more key-value pairs, where each key is groupID and its corresponding value is a generated group-dependent transaction.
      * Then we group all transactions by groupID.
      * Based on each group-dependent transaction, ParallelFPGrowthReducer build local FPTree and recursively mining frequent patterns from FPTree.
      * After that, we need to do two times map and one times groupByKey which have not been mentioned in paper to remove duplicate patterns.
      * For example, two group-dependent transactions may output the same pattern with different support: <"a" "b" "c", 4>, <"a" "b" "c", 2>. We only need the pattern with max support <"a" "b" "c", 4>.
      */
    val patterns = transactions.flatMap(line => ParallelFPGrowthMapper().map(line, fMap, maxPerGroup)).groupByKey.flatMap(line => ParallelFPGrowthReducer().reduce(line, minSupport)).map(line => (line._2, line._1)).groupByKey.map(line => (line._1, line._2.max))

    // Print results on the terminal.
    for(pattern <- patterns.collect){
      println(pattern._1 + " " + pattern._2)
    }

    // Return patterns.
    patterns
  }

  /**
   * For the convenience of dividing all items in fList to numGroups groups, we assign a unique ID which starts from 0 to (the length of fList - 1) to every item in fList.
   * @param fList Array of key-value pairs, where each key is item and its corresponding value is support.
   * @return HashMap, where key is item and value is ID.
   */
  def getFMap(fList: Array[(String, Int)]) : HashMap[String, Int] = {
    var i = 0
    val fMap = new HashMap[String, Int]()
    for(pair <- fList){
      fMap.put(pair._1, i)
      i += 1
    }
    fMap
  }

  /**
   * Compute the number of items in each group.
   * @param fList Array of key-value pairs, where each key is item and its corresponding value is support.
   * @param numGroups The user specified number of groups.
   * @return The number of items in each group.
   */
  def getMaxPerGroup(fList: Array[(String, Int)], numGroups: Int): Int = {
    var maxPerGroup = fList.length / numGroups
    if (fList.length % numGroups != 0) {
      maxPerGroup += 1
    }
    maxPerGroup
  }
}

object ParallelFPGrowthMapper{
  // Initialize ParallelFPGrowthMapper and return this instance.
  def apply(): ParallelFPGrowthMapper ={
    val mapper = new ParallelFPGrowthMapper()
    mapper
  }
}

class ParallelFPGrowthMapper(){
  /**
   * ParallelFPGrowthMapper generates group-dependent transactions - one or more key-value pairs, where each key is groupID and its corresponding value is a generated group-dependent transaction.
   * @param transaction One transaction which contains items.
   * @param fMap  HashMap, where key is item and value is ID.
   * @param maxPerGroup The number of items in each group.
   * @return Group-dependent transactions.
   */
  def map(transaction: Array[String], fMap: HashMap[String, Int], maxPerGroup: Int): ArrayBuffer[(Int, ArrayBuffer[String])] ={

    // Get the groupID of the item.
    def getGroupID(itemId: Int, maxPerGroup: Int): Int ={
      itemId / maxPerGroup
    }

    var retVal = new ArrayBuffer[(Int, ArrayBuffer[String])]()
    var itemArr = new ArrayBuffer[Int]()

    // Represent item by ID in the fMap and store all IDs in itemArr.
    for(item <- transaction){
      if(fMap.keySet.contains(item)){
        itemArr += fMap(item)
      }
    }

    // Sort IDs in the itemArr in ascending order. So, items which have smaller ID number and higher support will at the front position of itemArr.
    itemArr = itemArr.sortWith(_ < _)

    val groups = new ArrayBuffer[Int]()

    /**
     * Generate group-dependent transactions from the tail of itemArr:
     * 1. For each item in itemArr, substitute it by corresponding groupID.
     * 2. For each groupID, if it appears in itemArr, locate its RIGHT-MOST appearance and return all items in front of it.
     * For example, if we have one transaction <"a", "b", c"> and "a", "b" s' groupID is 1, "c" 's groupID is 2.
     * Then we return two pairs, <2, "a" "b" "c">, <1, "a" "b">. Note that we do NOT need to return <1, "a">.
     */
    for(i <- (0 until itemArr.length).reverse){
      val item = itemArr(i)
      val groupID = getGroupID(item, maxPerGroup)
      if(!groups.contains(groupID)){
        val tempItems = new ArrayBuffer[Int]()
        tempItems ++= itemArr.slice(0, i + 1)
        val items = tempItems.map(x => fMap.map(_.swap).getOrElse(x, null))
        retVal += groupID -> items
        groups += groupID
      }
    }
    retVal
  } // end of map

} // end of ParallelFPGrowthMapper

object ParallelFPGrowthReducer{
  // Initialize ParallelFPGrowthReducer and return this instance.
  def apply(): ParallelFPGrowthReducer ={
    val reducer = new ParallelFPGrowthReducer()
    reducer
  }
}

class ParallelFPGrowthReducer(){
  /**
   * Based on each group-dependent transaction, ParallelFPGrowthReducer build local FPTree and recursively mining frequent patterns from FPTree.
   * @param line One group-dependent transaction.
   * @param minSupport Support threshold.
   * @return  Patterns.
   */
  def reduce(line: (Int, Iterable[ArrayBuffer[String]]), minSupport: Int): ArrayBuffer[(Int, String)] ={
    val transactions = line._2
    val localFPTree = new LocalFPTree(new ArrayBuffer[(Int, String)]())
    localFPTree.FPGrowth(transactions, new ArrayBuffer[String], minSupport)
    localFPTree.patterns
  }
}

/**
 * FPTree is a tree structure defined below:
 * 1. It consists of one root labeled as “null”, a set of item-prefix subtrees as the children of the root, and a header table.
 * 2. Each node in the item-prefix subtree consists of three fields: item-name, count, and node-link,
 *    where item-name registers which item this node represents, count registers the number of transactions represented by the portion of the path reaching this node,
 *    and node-link links to the next node in the FP-tree carrying the same item-name, or null if there is none.
 * 3. Each entry in the header table consists of two fields, (1) item-name and (2) head of node-link (a pointer pointing to the first node in the FP-tree carrying the item-name).
 * @param patterns
 */
class LocalFPTree(var patterns: ArrayBuffer[(Int, String)]){
  /**
   * Build header table based on CURRENT transactions.
   * @param transactions
   * @param minSupport
   * @return Header table.
   */
  def buildHeaderTable(transactions: Iterable[ArrayBuffer[String]], minSupport: Int): ArrayBuffer[TreeNode] = {
    if(transactions.nonEmpty){
      val map: HashMap[String, TreeNode] = new HashMap[String, TreeNode]()

      // For every item in each transaction, count the number of occurrence.
      for(transaction <- transactions) {
        for(item <- transaction) {
          if(!map.contains(item)) {
            val node: TreeNode = new TreeNode(item)
            node.count = 1
            map(item) = node
          } else {
            map(item).count += 1
          }
        }
      }

      val headerTable = new ArrayBuffer[TreeNode]()

      // Filter map by minSupport and sort it by items' names in ascending order and suppports in descending order and copy map to header table.
      map.filter(_._2.count >= minSupport).values.toArray.sortWith(_.name < _.name).sortWith(_.count > _.count).copyToBuffer(headerTable)

      // Return headerTable.
      headerTable
    } else {
      null // If transactions is empty, return null.
    }
  } // end of buildHeaderTable

  /**
   * Build local FPTree based on CURRENT transactions.
   * @param transactions
   * @param headerTable
   * @return  FPTree
   */
  def buildLocalFPTree(transactions: Iterable[ArrayBuffer[String]], headerTable: ArrayBuffer[TreeNode]): TreeNode = {
    val root: TreeNode = new TreeNode()

    for(transaction <- transactions){
      // Sort items of each transaction by support in descending order.
      val sortedTransaction = sortByHeaderTable(transaction, headerTable)

      var subTreeRoot: TreeNode = root
      var tmpRoot: TreeNode = null

      if(root.children.nonEmpty){
        /**
         * If FPTree T has a child N such that N.name equals the name of item in sorted transaction, then increment N's support by 1.
         * Else create a new node N, with its support initialized to 1, its parent link linked to T, and its node-link linked to the nodes with the same item name via the node-link structure.
         */
        while(sortedTransaction.nonEmpty && subTreeRoot.findChild(sortedTransaction.head) != null){
          tmpRoot = subTreeRoot.children.find(_.name.equals(sortedTransaction.head)) match {
            case Some(node) => node
            case None => null
          }
          tmpRoot.count += 1
          subTreeRoot = tmpRoot

          // Remove the first item of sortedTransaction.
          sortedTransaction.remove(0)
        } // end of while
      }
      
      addNodes(subTreeRoot, sortedTransaction, headerTable)
    } // end of for

    /**
     * Sort items of transaction by support in descending order.
     * @param transaction
     * @param headerTable
     * @return
     */
    def sortByHeaderTable(transaction: ArrayBuffer[String], headerTable: ArrayBuffer[TreeNode]): ArrayBuffer[String] = {
      val map: HashMap[String, Int] = new HashMap[String, Int]()

      // Substitute item by index.
      for(item <- transaction){
        for(index <- 0 until headerTable.length){
          if(headerTable(index).name.equals(item)){
            map(item) = index
          }
        }
      }

      val sortedTransaction: ArrayBuffer[String] = new ArrayBuffer[String]()

      // Sort items by index in descending order and copy sorted items to sortedTransaction.
      map.toArray.sortWith(_._2 < _._2).foreach(sortedTransaction += _._1)

      // Return sortedTransaction.
      sortedTransaction
    } // end of sortByHeaderTable

    /**
     * Add nodes to FPTree.
     * @param parent
     * @param transaction
     * @param headerTable
     */
    def addNodes(parent: TreeNode, transaction: ArrayBuffer[String], headerTable: ArrayBuffer[TreeNode]){
      while(transaction.nonEmpty){
        // Get the first item of transaction and remove it from transaction.
        val name: String = transaction.head
        transaction.remove(0)

        // Create a new node.
        val leaf: TreeNode = new TreeNode(name)
        leaf.count = 1
        leaf.parent = parent
        parent.children += leaf

        // For breaking out of while loop.
        var cond = true

        var index: Int = 0

        // Update nextHomonym field of nodes in headertable.
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
      } // end of while
    } // end of addNodes

    root // return root
  } // end of buildTransactionTree

  /**
   * Mining frequent patterns recursively from FPTree.
   * @param transactions
   * @param prefix
   * @param minSupport
   */
  def FPGrowth(transactions: Iterable[ArrayBuffer[String]], prefix: ArrayBuffer[String], minSupport: Int){

    // Build header table based on CURRENT transactions.
    val headerTable: ArrayBuffer[TreeNode] = buildHeaderTable(transactions, minSupport)

    // Build local FPTree based on CURRENT transactions.
    val treeRoot = buildLocalFPTree(transactions, headerTable)

    if(treeRoot.children.nonEmpty){
      // Output frequent patterns.
      if(prefix.nonEmpty){
        for(node <- headerTable){
          var result: String = ""
          val temp = new ArrayBuffer[String]()
          temp += node.name
          for(pattern <- prefix){
            temp += pattern
          }

          // Sort items by item name.
          result += temp.sortWith(_ < _).mkString(" ").toString

          // If result already exists in patterns, then compare support. We only need the pattern with max support.
          val index = patterns.map(_._2).indexOf(result)
          if (index != -1) {
            if (patterns(index)._1 <= node.count) {
              patterns.remove(index)
              patterns += node.count -> result
            }
          } else {
            patterns += node.count -> result
          }
        }
      }

      for (node: TreeNode <- headerTable) {
        // Initialize new prefix with current node name.
        val newPrefix: ArrayBuffer[String] = new ArrayBuffer[String]()
        newPrefix += node.name

        // Add old prefix to new prefix.
        if (prefix.nonEmpty)
          newPrefix ++= prefix

        val newTransactions: ArrayBuffer[ArrayBuffer[String]] = new ArrayBuffer[ArrayBuffer[String]]()
        var backNode: TreeNode = node.nextHomonym

        while (backNode != null) {
          // Get the support of current node.
          var counter: Int = backNode.count

          val preNodes: ArrayBuffer[String] = new ArrayBuffer[String]()
          var parent: TreeNode = backNode.parent

          // Find all paths from root to current node.
          while (parent.name != null) {
            preNodes += parent.name
            parent = parent.parent
          }
          // Set the support of parent nodes of current node the same as the support of current node.
          while (counter > 0) {
            newTransactions += preNodes
            counter -= 1
          }
          backNode = backNode.nextHomonym
        }

        FPGrowth(newTransactions, newPrefix, minSupport)
      } // end of for

    } // end of if
  } // end of FPGrowth
} // end of LocalFPTree
