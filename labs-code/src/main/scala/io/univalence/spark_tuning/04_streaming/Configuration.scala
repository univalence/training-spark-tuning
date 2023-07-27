package io.univalence.spark_tuning.`04_streaming`

object Configuration {
  val inputTopic = "input-topic"
  val outputTopic = "output-topic"

  val bootstrapServers = "localhost:9092"

  val partitionCount = 8
  val replicationFactor: Short = 1.toShort
}
