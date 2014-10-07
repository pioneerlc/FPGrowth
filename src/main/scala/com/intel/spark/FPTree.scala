package com.intel.spark

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.Buffer

/**
 * TreeNode.scala
 * This is the definition of FP-Tree.
 * Author: Mark Lin
 * E-mail: chlin.ecnu@gmail.com
 * Version: 1.0
 */

object FPTree {
  def apply(records: Array[Array[String]], supportThreshold: Int): Unit ={
    var transactions: Buffer[Buffer[String]] = new ArrayBuffer[Buffer[String]]()
    for(record <- records){
      transactions += record.toBuffer
    }
    val fptree = new FPTree()
    fptree.FPGrowth(transactions, new ArrayBuffer[String](), supportThreshold)
  }
}

class FPTree{
  def buildHeaderTable(transactions: Buffer[Buffer[String]], supportThreshold: Int): Array[TreeNode] = {

    if(transactions.nonEmpty){
      val map: HashMap[String, TreeNode] = new HashMap[String, TreeNode]()
      for(transaction <- transactions){
        for(name <- transaction){
          if(!map.keySet.contains(name)){
            val node: TreeNode = new TreeNode(name)
            node.count = 1
            map.put(name, node)
          }else{
            map(name).count += 1
          }
        }
      }

      val headerTable = map.filter(_._2.count >= supportThreshold).values.toArray.sortWith(_.count > _.count)
      headerTable //return headerTable
    }else{
      null //if transactions is empty, return null
    }
  }

  def buildFPTree(transactions: Buffer[Buffer[String]], headerTable: Buffer[TreeNode]): TreeNode = {
    val root: TreeNode = new TreeNode()

    for(index <- 0 until transactions.length){
      val transaction = (transactions(index)).toBuffer //in case of reassignment, we convert Array to Buffer
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
        }
      }
      addNodes(subTreeRoot, sortedTransaction, headerTable)
    }

    def sortByHeaderTable(transaction: Buffer[String], headerTable: Buffer[TreeNode]): ArrayBuffer[String] = {
      val map: HashMap[String, Int] = new mutable.HashMap[String, Int]()
      for(item <- transaction){
        for(index <- 0 until headerTable.length){
          if(headerTable(index).name.equals(item)){
            map.put(item, index)
          }
        }
      }

      val sortedTransaction: ArrayBuffer[String] = new ArrayBuffer[String]()
      map.toArray.sortWith(_._2 < _._2).foreach(sortedTransaction += _._1)
      sortedTransaction //return sortedTransaction
    }

    def addNodes(parent: TreeNode, transaction: Buffer[String], headerTable: Buffer[TreeNode]): Unit = {
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
    }

    root // return root
  }

  def FPGrowth(transactions: Buffer[Buffer[String]], postPattern: ArrayBuffer[String], supportThreshold: Int): Unit = {

    val headerTable: Buffer[TreeNode] = buildHeaderTable(transactions, supportThreshold).toBuffer
    val treeRoot = buildFPTree(transactions, headerTable)
    if(treeRoot.children.nonEmpty){
      if(postPattern.nonEmpty){
        var result: String = ""
        for(node <- headerTable){
          result += "Frequency: " + node.count + " " + node.name +  " "
          for(pattern <- postPattern){
            result += pattern + " "
          }
          result += "\n"
        }
        println(result)
      }

    for (node: TreeNode <- headerTable) {
      val newPostPattern: ArrayBuffer[String] = new ArrayBuffer[String]()
      newPostPattern += node.name
      if (postPattern.nonEmpty)
        newPostPattern ++= postPattern
      val newTransactions: Buffer[Buffer[String]] = new ArrayBuffer[Buffer[String]]()
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
    }

    }
  }
}


