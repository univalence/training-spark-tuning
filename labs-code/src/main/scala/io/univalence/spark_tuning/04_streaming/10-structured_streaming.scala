package io.univalence.spark_tuning.`04_streaming`

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{OutputMode, StreamingQuery}
import org.apache.spark.sql.types._

object `10-structured_streaming` {

  def main(args: Array[String]): Unit = {
    val spark =
      SparkSession
        .builder()
        .appName("StructuredStreamingApp")
        .master("local[*]")
        .getOrCreate()

    import spark.implicits._

    val rawDf: DataFrame =
      spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", Configuration.bootstrapServers)
        .option("subscribe", Configuration.inputTopic)
        .load()

    val orderSchema: StructType =
      StructType(
        Seq(
          StructField("id", StringType),
          StructField("client", StringType),
          StructField("timestamp", TimestampType),
          StructField("product", StringType),
          StructField("price", DoubleType)
        )
      )

    val df: DataFrame =
      rawDf
        .select(
          $"key".cast("STRING").as("key"),
          $"value".cast("STRING").as("value")
        )
        .select(
          $"key",
          from_csv(
            $"value",
            orderSchema,
            Map.empty[String, String]
          ).as("value")
        )

    df.printSchema()

    val resultDf: DataFrame =
      df
        .groupBy($"value.product".as("key"))
        .count()
        .select($"key", $"count".cast("STRING").as("value"))

    val ds: StreamingQuery =
      resultDf.writeStream
        .outputMode(OutputMode.Update())
        .format("kafka")
        .option("kafka.bootstrap.servers", Configuration.bootstrapServers)
        .option("topic", Configuration.outputTopic)
        .option("checkpointLocation", "data/output/checkpoint")
        .start()

    ds.awaitTermination()
  }

}


