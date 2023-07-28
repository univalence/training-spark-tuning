package io.univalence.spark_tuning._03_spark_sql

import org.apache.spark.sql.SparkSession

/**
 * Spark SQL may generate Java code from your queries. This code is then
 * compile with [[https://janino-compiler.github.io/janino/ Janino]],
 * which is a lightweight and embeddable Java compiler.
 *
 * It is possible for a request to see the generated code.
 */
object _30_codegen {
  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .appName("StationApp")
        .master("local[*]")
        .getOrCreate()

    val data =
      spark.read
        .option("header", "true")
        .csv("data/stations.csv")

    val result =
      data
        .select("*")
        .where(data("station_rating") >= 4.0)
        .orderBy(data("station_rating").desc)

    // import the .debugCodegen() method
    import org.apache.spark.sql.execution.debug._

    // output the generated Java code
    result.debugCodegen()

    /**
     *
     */
  }
}
