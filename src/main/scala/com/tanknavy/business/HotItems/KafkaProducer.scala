package com.tanknavy.business.HotItems

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.BufferedSource

/**
 * Author: Alex Cheng 12/3/2020 10:36 AM
 */

//测试用，读取本地文件发给kafka
object KafkaProducer {
  def main(args: Array[String]): Unit = {
    //writeToKafka("hotitems")
    writeToKafka("orderLog")
  }

  def writeToKafka(topic: String):Unit = {
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    //props.setProperty("group.id", "consumer-flink") //消费组
    props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    //props.setProperty("auto.offset.reset","latest") //偏移量自动重置

    //定义kafka producer
    val producer = new KafkaProducer[String, String](props) //[k,v]

    //从文件中读取数据，发送给kafka
    val bufferredSource: BufferedSource = io.Source.fromFile("D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\UserBehavior.csv")
    //遍历buffer每一行
    for( line <- bufferredSource.getLines()){
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close() //发送后关闭



  }
}
