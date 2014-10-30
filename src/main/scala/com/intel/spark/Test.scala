package com.intel.spark

object Test {
  def main (args: Array[String]) {
    val data = Array(Array("f", "c", "a", "d", "g", "i", "m", "p"), Array("a", "b", "c", "f", "l", "m", "o"), Array("b", "f", "h", "j", "o"), Array("b", "c", "k", "s", "p"), Array("a", "f", "c", "e", "l", "p", "m", "n"), Array("f", "c", "a", "d", "g", "i", "m", "p"), Array("a", "b", "c", "f", "l", "m", "o"), Array("b", "f", "h", "j", "o"), Array("b", "c", "k", "s", "p"), Array("a", "f", "c", "e", "l", "p", "m", "n"))
    FPTree(data, 3)
  }
}
