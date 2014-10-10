package com.intel.spark

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap

/**
 * FPTree.scala
 * This is the definition of FP-Tree.
 * Author: Mark Lin
 * E-mail: chlin.ecnu@gmail.com
 * Version: 1.1
 */

object FPTree{
  def apply(records: Array[Array[String]], supportThreshold: Int): FPTree = {
    var transactions = new ArrayBuffer[ArrayBuffer[String]]()
    for(record <- records){
      var transaction = new ArrayBuffer[String]()
      record.copyToBuffer(transaction)
      transactions += transaction
    }
    val fptree = new FPTree(new ArrayBuffer[String]())
    fptree.FPGrowth(transactions, new ArrayBuffer[String](), supportThreshold)
    fptree
  }
} //end of object FPTree

class FPTree(var patterns: ArrayBuffer[String]){

  def buildHeaderTable(transactions: ArrayBuffer[ArrayBuffer[String]], supportThreshold: Int): ArrayBuffer[TreeNode] = {
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

  def buildFPTree(transactions: ArrayBuffer[ArrayBuffer[String]], headerTable: ArrayBuffer[TreeNode]): TreeNode = {
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
  } //end of buildFPTree

  def FPGrowth(transactions: ArrayBuffer[ArrayBuffer[String]], postPattern: ArrayBuffer[String], supportThreshold: Int){
    val headerTable: ArrayBuffer[TreeNode] = buildHeaderTable(transactions, supportThreshold)

    val treeRoot = buildFPTree(transactions, headerTable)

    if(treeRoot.children.nonEmpty){
      if(postPattern.nonEmpty){
        var result: String = ""
        for(node <- headerTable){
          result += "Frequency: " + node.count + " " + node.name +  " "
          print("Frequency: " + node.count + " " + node.name +  " ")
          for(pattern <- postPattern){
            result += pattern + " "
            print(pattern + " ")
          }
          result += "\n"
          print("\n")
        }
        patterns += result
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
} //end of FPTree


