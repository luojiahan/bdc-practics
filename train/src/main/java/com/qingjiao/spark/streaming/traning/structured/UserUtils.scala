package com.qingjiao.spark.streaming.traning.structured

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.json4s.DefaultFormats
import org.json4s.jackson.Json

import java.util.{Properties, Random}

object UserUtils {
  // 创建Kafka生产者
  def getKafkaProducer(): KafkaProducer[String, String] = {
    val properties = new Properties()
    properties.setProperty("bootstrap.servers","hadoop:9092")
    properties.setProperty("ack","1")
    properties.setProperty("retries","3")
    properties.setProperty("key.serializer","org.apache.kafka.common.serialization.StringSerializer")
    properties.setProperty("value.serializer","org.apache.kafka.common.serialization.StringSerializer")

    new KafkaProducer[String, String](properties)
  }

  // 创建Kafka消费者
  def getKafkaConsumer(spark:SparkSession,topic:String): DataFrame = {
    spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers","hadoop:9092")
      .option("subscribe",topic)
      .load()
  }

  // 模拟数据产生
  def mockData(topic:String): Unit = {
    val random = new Random()
    // 模拟设备类型数据
    val deviceTypes = Array("mysql", "redis", "kafka", "route", "redis", "flume", "mysql", "kafka", "mysql")
    val json = new Json(DefaultFormats)

    // 获取生产者对象
    val producer = getKafkaProducer()
    while (true) {
      val index: Int = random.nextInt(deviceTypes.length)
      val deviceId: String = s"device_${(index + 1) * 10 + random.nextInt(index + 1)}"
      val deviceType = deviceTypes(index)
      val deviceSignal = 10 + random.nextInt(90)
      val deviceData = DeviceData(deviceId, deviceType, deviceSignal, System.currentTimeMillis())

      // case class => json string
      val jsonString = json.write(deviceData)

      val record = new ProducerRecord[String, String](topic, jsonString)

      // 将数据发送到指定的topic
      producer.send(record)

      Thread.sleep(2000)
    }
  }

}
