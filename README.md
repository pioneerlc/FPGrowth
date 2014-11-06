FPGrowth
========

This is a naive implementation of FPGrowth for learning how to use Spark and Scala. 

Author: Mark Lin

E-mail: chlin.ecnu@gmail.com


This project contains files as follows:

FPGrowth.scala: entry point when running on spark, including initialzing spark, loading file from hdfs, calling FPTree

FPTree.scala: main implementation of FPGrowth, including build header-table, build FPTree, mining frequent patterns by traversing FPTree

Test.scala: entry point when testing locally

TreeNode.scala: data structure of tree node of FPTree

ParallelFPGrowth.scala: main implementation of parallel FPGrowth, including computing the frequency list of transactions database, dividing transactions into Q groups, mining from local FPTree


Revision log
========
Version 1.0:
This verison of FPGrowth can be successfully run by Test.scala. However, it can not print results on the terminal when running on Spark1.1.0. I will try to fix this problem later.

Version 1.1:
This verison of FPGrowth fix the problem of version 1.0, which can not print anything on the terminal.

Version 2.0:
In this version, I add ParallelFPGrowth.scala into this project, which has been partly accomplished. Until now, computing the frequency list of transactions database and dividing
transactions into Q groups have been implemented in ParallelFPGrowth.scala. I will add the remaining features later.

Version 2.1:
In this version, ParallelFPGrowth.scala has been implemented completely.

Version 2.2:
In this version, we override the strategy of how to dividing transactions to several groups.

Version 2.3:
In this version, we fix one bug which might lead to obtain duplicate patterns which have same items but different support value.
