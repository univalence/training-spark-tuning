package io.univalence.spark_tuning

import org.apache.spark.sql._
import scala.util.Using
import java.io.PrintWriter
import java.nio.charset.StandardCharsets

object Get { // java -cp ... Get csv cluster_file local_file
  def main(args: Array[String]): Unit = {
    if (args.length != 3) {
      println("you should give only two parameter (format + cluster input file + local output file)")
      sys.exit(1)
    }

    val format     = args(0)
    val filename   = args(1)
    val outputName = args(2)

    val spark = SparkSession.builder().appName("Get").master("local[*]").getOrCreate()
    val rdd = spark.read.format(format).load(filename).rdd.map(_.mkString(","))

    Using(new PrintWriter(outputName, StandardCharsets.UTF_8)) { file =>
      rdd.toLocalIterator.foreach(line => file.println(line))
    }.get
  }
}
