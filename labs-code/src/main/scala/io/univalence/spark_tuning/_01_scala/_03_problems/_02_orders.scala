package io.univalence.spark_tuning._01_scala._03_problems

import scala.io.Source
import scala.util.Using

import java.time.LocalDateTime

object _02_orders {

  case class Order(
      id:        String,
      client:    String,
      timestamp: LocalDateTime,
      product:   String,
      price:     Double
  )

  def main(args: Array[String]): Unit = {
    val filename = "data/order.csv"

    val lines: Iterator[String] = scanCsv(filename)
    val orders: Iterator[Order] = lines.map(lineToOrder)
  }

  private def lineToOrder(line: String): Order = {
    val fields = line.split(",").toList

    Order(
      id = fields(0),
      client = fields(1),
      timestamp = LocalDateTime.parse(fields(2)),
      product = fields(3),
      price = fields(4).toDouble
    )
  }

  private def scanCsv(filename: String): Iterator[String] =
    Using(Source.fromFile(filename)) { file =>
      file.getLines().toList
    }.get.iterator

}
