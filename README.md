FPGrowth
========

This is a navie implementation of FPGrowth for learning how to use Spark and Scala. 

Author: Mark Lin

E-mail: chlin.ecnu@gmail.com



This project contains files as follows:

FPGrowth.scala: entry point when running on spark, initialzing spark, load file from hdfs, call FPTree

FPTree.scala: main implementation of FPGrowth， build header-table, build FPTree, mining frequent patterns by traversing FPTree

Test.scala: entry point when testing locally

TreeNode.scala: data structure of tree node of FPTree



Revision log
========
Version 1.0：
This verison of FPGrowth can be successfully run by Test.scala. However, it can not print results on the terminal when running on Spark1.1.0. I will try to fix this problem later.




