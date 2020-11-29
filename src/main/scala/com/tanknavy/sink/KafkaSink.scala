package com.tanknavy.sink

import java.util.Properties

import com.tanknavy.source.bounded.SensorReading
import com.tanknavy.source.kafka.SensorSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

/**
 * Author: Alex Cheng 11/28/2020 8:31 PM
 */

//end-to-end状态一致性
//kafka source(topic: orderLog) -> Flink -> kafka sink(topic: first-topic)
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1) //在本地开发环境中，默认并行度是core数量个
    //实际常见是kafka source->flink->kafka sink
    //文本文件数据源
    //val streamFromFile = env.readTextFile("D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    //自定义类型数据源
    //val mySourceStream: DataStream[SensorReading] = env.addSource(new SensorSource()) //自定义数据源

    //2.配置kafka consumer client
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "consumer-flink") //消费组
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serializations.StringDeserializer")
    props.setProperty("auto.offset.reset","latest") //偏移量自动重置

    //3.消费kafka数据源，addSource()
    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("orderLog",
      new SimpleStringSchema(),props)) //kafka-topic, valueDeserializer, props


    //1.基本转换算子和简单聚合算子, keyBy: DataStream -> KeyedStream, 然后可以agg/reduce
    //val dataStream = streamFromFile.map(
    //val dataStream = mySourceStream.map(
    val dataStream = kafkaStream.map(
      data =>{
        val dataArray = data.split(",")
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //为了方便序列化写到kafka
      })


    //sink到kafka, flink官方实现了kafka的sink
    dataStream.addSink(new FlinkKafkaProducer011[String]("localhost:9092","first-topic", new SimpleStringSchema()))
    dataStream.print()

    env.execute("kafka sink test")

  }
}
