package io.univalence.spark_tuning.`02_spark_rdd`

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import scala.io.Source
import scala.util.Using

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

object `20-broadcast` {

  def main(args: Array[String]): Unit = {
    val orderFilename         = "data/orders.csv"
    val clientMappingFilename = "data/client-mapping.csv"

    val conf = new SparkConf()
    conf.setAppName("CafeteriaBroadcastApp")
    // conf.setMaster("spark://localhost:7077")
    conf.setMaster("local[*]")

    val historyPath = "target/history"
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", historyPath)

    val spark = SparkContext.getOrCreate(conf)

    val fileRdd: RDD[String] =
      spark.parallelize(
        Using(Source.fromFile(orderFilename)) { file =>
          file.getLines().toSeq
        }.get,
        numSlices = 4
      )
//      spark.textFile(orderFilename, minPartitions = 4)

    // get the header
    val header = fileRdd.first()

    // Transformation: convert line into order
    val orderRdd: RDD[Order] =
      fileRdd
        // only way to remove header
        .filter(_ != header)
        .map(convertLine)

    val mapping: Map[String, String] =
      Using(Source.fromFile(clientMappingFilename)) { file =>
        (
          for (line <- file.getLines().drop(1)) yield {
            val fields = line.split(",")
            fields(0) -> fields(1)
          }
        ).toMap
      }.get

    val result: List[Order] =
      orderRdd // Ranking of the most ordered products
        .map(order => order.copy(clientId = mapping.getOrElse(order.clientId, order.clientId)))
        .collect()
        .toList

    result.take(100).foreach(println)
  }

  def convertLine(line: String): Order = {
    val fields = line.split(",").toList

    Order(
      id       = fields(0),
      clientId = fields(1),
      timestamp =
        LocalDateTime.parse(
          fields(2),
          DateTimeFormatter.ISO_LOCAL_DATE_TIME
        ),
      product = fields(3),
      price   = fields(4).toDouble
    )
  }

  case class Order(
      id:        String,
      clientId:  String,
      timestamp: LocalDateTime,
      product:   String,
      price:     Double
  )

}
