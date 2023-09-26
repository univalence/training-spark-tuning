package io.univalence.spark_tuning._02_spark_rdd

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.io.Source
import scala.util.Using

/**
 * Here is the first example of application that uses Spark (more
 * precisely, Spark Core).
 *
 * Spark is a framework that aims to help you define application able to
 * process big data.
 *
 * The main abstraction of Spark is the RDD (Resilient Distributed
 * Dataset), which can be viewed as a distributed collection.
 */
object _10_rdd {
  def main(args: Array[String]): Unit = {
    val filename = "data/orders.csv"

    /** We first need to create a configuration for Spark. */
    val conf = new SparkConf()

    /**
     * When you create a Spark context, you have to give a name to your
     * Spark application. This name will be used in logs and in the
     * monitoring interface. It is important enough to retrieve the
     * metrics of your Spark application in certain Spark tools.
     */
    conf.setAppName("CafeteriaApp")

    /**
     * You also have to indicate the master node of your Spark cluster.
     * It is a string that can have many forms:
     *
     *   - `spark://<hostname>:<port>`, for standalone mode.
     *   - `yarn` (yes, just `yarn`), if the Spark cluster is deployed
     *     on Hadoop. In this case, you have to configure
     *     `HADOOP_CONF_DIR` or `YARN_CONF_DIR` to locate the file with
     *     the full configuration to find and describe the Hadoop
     *     cluster.
     *   - `k8s://<hostname>:<port>`, if the Spark cluster is deployed
     *     on Kubernetes.
     *
     * If you only want to run Spark locally, with no Spark cluster, you
     * can specify `local[*]` for the master, like below. The `*`
     * indicates that Spark will use the available CPU cores for its
     * processing. Instead of `*`, you can specify a number, eg.
     * `local[2]` to limit Spark to 2 CPU cores.
     */
    conf.setMaster("local[*]")
//    conf.setMaster("spark://localhost:7077")

    val historyPath = "target/history"
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.eventLog.dir", historyPath)

    /**
     * To use Spark, you have to first create a Spark context. A Spark
     * context is a that makes Spark communicate with the Spark cluster
     * or other kind of cluster (if one is configured). It is also used
     * to configure Spark, and to initiate the Spark application.
     */
    val spark = SparkContext.getOrCreate(conf)

    // load the file
     val fileRdd: RDD[String] = spark.textFile(filename, minPartitions = 4)
//    val lines =
//      Using(Source.fromFile(filename)) { file =>
//        file.getLines().toList
//      }.get
//    val fileRdd: RDD[String] = spark.parallelize(lines, numSlices = 4)

//    fileRdd.foreachPartition(partition => partition.foreach(line => println(line)))
//    fileRdd.repartition(4)
//    fileRdd.coalesce(4)

    // get the header
    val header: String = fileRdd.first()

    // Transformation: convert line into order
    val orderRdd: RDD[Order] =
      fileRdd
        // only way to remove header
        .filter(_ != header)
        .map(convertLine)

    val result: List[(String, Int)] =
      orderRdd // Ranking of the most ordered products
        .keyBy(_.product)
        .mapValues(_ => 1)
        .reduceByKey((total, subTotal) => total + subTotal)
        .sortBy(_._2, ascending = false)
        .collect()
        .toList

    // Display the ranking
    for (line <- result)
      println(line)
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
