package io.univalence.spark_tuning._05_mllib

import org.apache.spark.sql._

object _xx_xx {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .appName("MlLib - Features")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    val filename = "data/tweets.json.gz"
    val tweets = spark.read.json(filename)

    tweets.show()
  }
}
