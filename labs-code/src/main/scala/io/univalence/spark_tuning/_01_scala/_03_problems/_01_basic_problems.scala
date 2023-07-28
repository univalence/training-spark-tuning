package io.univalence.spark_tuning._01_scala._03_problems

import io.univalence.spark_tuning.internal.exercise_tools._

object _01_basic_problems {
  def main(Args: Array[String]): Unit = {
      exercise("salary increase") {
    def increase(salaries: List[Double], rate: Double): List[Double] = |>?

    check(increase(List(1000.0, 2000.0, 3500.0), 0.02) == List(1020.0, 2040.0, 3570.0))
  }

  exercise("average") {
    def average(values: Iterator[Double]): !? = |>?

    check(average(Iterator.empty) == ??)
    check(average(Iterator(1.0)) == ??)
    check(average(Iterator(1.0, 3.0)) == ??)
  }

  exercise("recursive factorial") {
    def factorial(n: Int): Int = |>?

    check(factorial(0) == 1)
    check(factorial(1) == 1)
    check(factorial(2) == 2)
    check(factorial(3) == 6)
    check(factorial(4) == 24)
  }

  exercise("recursive Fibonacci sequence") {

    /**
     * The Fibonacci sequence is a sequence where the value of an
     * iteration is the sum of the value of the two previous iterations.
     * The two first values are 1 and 1.
     *
     * @param n
     *   n should be > 0
     */
    def fibonacci(n: Int): Int = |>?

    check(fibonacci(0) == 1)
    check(fibonacci(1) == 1)
    check(fibonacci(2) == 3)
    check(fibonacci(3) == 5)
    check(fibonacci(4) == 8)
    check(fibonacci(5) == 13)
  }

  exercise("word count") {
    def wordCount(test: String): Map[String, Int] = |>?

    check(wordCount("") == Map.empty)
    check(wordCount("ab") == Map("ab" -> 1))
    check(wordCount("ab ab") == Map("ab" -> 2))
    check(wordCount("ab cd") == Map("ab" -> 1, "cd" -> 1))
    check(wordCount("ab cd ab ef ef ef") == Map("ab" -> 2, "cd" -> 1, "ef" -> 3))
  }

  exercise("anagram") {
    def isPalindrome(word: String): Boolean = |>?

    check(isPalindrome(""))
    check(isPalindrome("a"))
    check(isPalindrome("aa"))
    check(!isPalindrome("ab"))
    check(isPalindrome("abba"))
    check(isPalindrome("radar"))
  }

  sealed trait Tree[+A]

  object Tree {
    final case object Leaf extends Tree[Nothing]
    final case class Node[A](value: A, left: Tree[A], right: Tree[A]) extends Tree[A]
    
    def simpleNode[A](value: A): Tree[A] = Tree.Node(value, Tree.Leaf, Tree.Leaf)
  }

  import Tree._

  exercise("size of a tree") {
    def size[A](tree: Tree[A]): Int = |>?

    check(size(Leaf) == 0)
    check(size(simpleNode(1)) == 1)
    check(size(Node(1, Leaf, simpleNode(2))) == 2)
    check(size(Node(1, Leaf, Node(2, simpleNode(3), simpleNode(4)))) == 4)
  }

  exercise("size of the longuest path of a tree") {
    def longuestPath[A](tree: Tree[A]): Int = |>?

    check(longuestPath(Leaf) == 0)
    check(longuestPath(simpleNode(1)) == 1)
    check(longuestPath(Node(1, Leaf, simpleNode(2))) == 2)
    check(longuestPath(Node(1, Leaf, Node(2, simpleNode(3), simpleNode(4)))) == 3)
    check(longuestPath(Node(1, simpleNode(5), Node(2, simpleNode(3), simpleNode(4)))) == 3)
  }

  exercise("make string, depth first approach - v1") {
    def mkString[A](tree: Tree[A], separator: String): String = |>?

    check(mkString(Leaf, ", ") == "")
    check(mkString(simpleNode("a"), ", ") == "a")
    check(mkString(Node("a", simpleNode("b"), Leaf), ", ") == "a, b")
    check(mkString(Node("a", Leaf, simpleNode("b")), ", ") == "a, b")
    check(mkString(Node("a", simpleNode("b"), simpleNode("c")), ", ") == "a, b, c")
    check(
      mkString(Node("a", simpleNode("b"), Node("c", simpleNode("d"), simpleNode("e"))), ", ")
        == "a, b, c, d, e"
    )
  }

  exercise("make string, depth first approach - v2") {
    def mkString[A](tree: Tree[A], separator: String): String = |>?

    check(mkString(Leaf, ", ") == "")
    check(mkString(simpleNode("a"), ", ") == "a")
    check(mkString(Node("a", simpleNode("b"), Leaf), ", ") == "a, b")
    check(mkString(Node("a", Leaf, simpleNode("b")), ", ") == "b, a")
    check(mkString(Node("a", simpleNode("b"), simpleNode("c")), ", ") == "b, a, c")
    check(
      mkString(Node("a", simpleNode("b"), Node("c", simpleNode("d"), simpleNode("e"))), ", ")
        == "b, a, d, c, e"
    )
  }

  exercise("make string, depth first approach - v3") {
    def mkString[A](tree: Tree[A], separator: String): String = |>?

    check(mkString(Leaf, ", ") == "")
    check(mkString(simpleNode("a"), ", ") == "a")
    check(mkString(Node("a", simpleNode("b"), Leaf), ", ") == "b, a")
    check(mkString(Node("a", Leaf, simpleNode("b")), ", ") == "b, a")
    check(mkString(Node("a", simpleNode("b"), simpleNode("c")), ", ") == "b, c, a")
    check(
      mkString(Node("a", simpleNode("b"), Node("c", simpleNode("d"), simpleNode("e"))), ", ")
        == "b, d, e, c, a"
    )
  }
  }
}
