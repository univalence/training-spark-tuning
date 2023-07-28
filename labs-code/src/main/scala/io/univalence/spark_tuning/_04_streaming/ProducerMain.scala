package io.univalence.spark_tuning._04_streaming

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.Serdes

import scala.annotation.tailrec
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.util.{Random, Using}

object ProducerMain {
  def main(args: Array[String]): Unit = init(Configuration.inputTopic, Configuration.outputTopic)

  Using(
    new KafkaProducer[String, String](
      Map[String, AnyRef](
        ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> Configuration.bootstrapServers
      ).asJava,
      Serdes.String().serializer(),
      Serdes.String().serializer()
    )
  ) { producer =>
    Using(Source.fromFile("data/orders.csv")) { file =>
      for (line <- file.getLines().drop(1)) {
        val fields = line.split(",")
        val record = new ProducerRecord[String, String](Configuration.inputTopic, fields(0), line)

        val metadata: RecordMetadata = producer.send(record).get()

        println(s"$metadata -> $line")
        val time = Random.nextInt(500) + 200
        Thread.sleep(time)
      }
    }
  }.get

  def init(inputTopic: String, outputTopic: String): Unit =
    Using(
      AdminClient.create(
        Map[String, AnyRef](
          AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> Configuration.bootstrapServers
        ).asJava
      )
    ) { admin =>
      val topics        = Set(inputTopic, outputTopic)
      val currentTopics = admin.listTopics().names().get().asScala

      if (!currentTopics.contains(inputTopic)) {
        val input = new NewTopic(inputTopic, Configuration.partitionCount, Configuration.replicationFactor)
        admin.createTopics(List(input).asJava).all().get()

      }
      if (!currentTopics.contains(outputTopic)) {
        val output = new NewTopic(outputTopic, Configuration.partitionCount, Configuration.replicationFactor)
        admin.createTopics(List(output).asJava).all().get()
      }

      if (
        !retry {
          val kafkaTopics = admin.listTopics().names().get().asScala
          topics.subsetOf(kafkaTopics)
        }()
      ) throw new RuntimeException(s"unable to create topics: ${topics.mkString(", ")}")
    }.get

  @tailrec
  def retry(f: => Boolean)(maxRetry: Int = 5, delay: Int = 500): Boolean =
    if (maxRetry <= 0) false
    else if (f) true
    else {
      Thread.sleep(delay)
      retry(f)(maxRetry - 1, delay)
    }

}
