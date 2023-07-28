package io.univalence.spark_tuning._04_streaming

object Configuration {
  val inputTopic = "input-topic"
  val outputTopic = "output-topic"

  val bootstrapServers = "localhost:9092"

  val partitionCount = 8
  val replicationFactor: Short = 1.toShort
}
