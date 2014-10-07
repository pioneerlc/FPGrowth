package com.intel.spark

import scala.collection.mutable.ArrayBuffer

/**
 * TreeNode.scala
 * This is the definition of TreeNode of FP-Tree
 * Author: Mark Lin
 * E-mail: chlin.ecnu@gmail.com
 * Version: 1.0
 */

class TreeNode (val name: String = null, var count: Int = 0, var parent: TreeNode = null, val children: ArrayBuffer[TreeNode] = new ArrayBuffer[TreeNode](),  var nextHomonym: TreeNode = null){
  def findChild(name: String): TreeNode = {
    children.find(_.name == name) match {
      case Some(node) => node
      case None => null
    }
  }
}
