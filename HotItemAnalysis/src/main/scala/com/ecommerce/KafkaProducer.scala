package com.ecommerce

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.io.BufferedSource

object KafkaProducer {

  def writeToKafka(topic: String): Unit = {

    val properties: Properties = new Properties()
    properties.put("bootstrap.servers", "master1.yunda:9092,slave1.yunda:9092,slave2.yunda:9092")
    properties.put("acks", "all")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("group.id", "hot-item-group-id")

    val producer: KafkaProducer[String, String] = new KafkaProducer[String, String](properties)
    val bufferedSource: BufferedSource = io.Source.fromFile("D:\\workspace\\UserBehaviorAnalysis\\HotItemAnalysis\\src\\main\\resources\\UserBehavior.csv")

    for (line <- bufferedSource.getLines()){
      val record: ProducerRecord[String, String] = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.flush()
  }

  def main(args: Array[String]): Unit = {
    writeToKafka("hot_item")
  }
}
