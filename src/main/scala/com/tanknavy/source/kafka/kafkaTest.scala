package com.tanknavy.source.kafka

import java.util.Properties

import com.tanknavy.source.bounded.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

/**
 * Author: Alex Cheng 11/28/2020 11:41 AM
 */

object kafkaTest {
  def main(args: Array[String]): Unit = {
    //1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

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

    //3.1 如何保证消费时exactly-once？
    // spark从kafka拉取mini-batch数据(kafka中offset更新)，如果这时spark挂了存在数据丢失风险，方法一不要自动提交，改为spark手动提交offset(消费完成提交偏移量)，
    // 方法二spark检查点保存进来的数据，在恢复spark时手动修改offset,相当于告诉kafka从某个指定offset开始
    // 但是在flink中，flink不是像spark一批一批而是一条一条处理，而且flink是有状态的流处理，所以flink可以把kafka当前offset当做状态保存下来，flink已经实现了这些



    //4.输出流
    kafkaStream.print("kafka-stream").setParallelism(1)

    //5.执行流处理
    env.execute("kafka-test")


  }
}

//自定义source类，实现SourceFunction接口，泛型
class SensorSource() extends SourceFunction[SensorReading]{
  //定义flag表示数据源是否正常运行
  var running:Boolean = true

  //正常生成数据
  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //初始化随机数生成器
    val rand = new Random()

    //初始化定义一组传感器温度数据
    var curTemp = 1.to(10).map(
      i =>("sensor_" + i, 32 + rand.nextGaussian() * 20)
    )

    //连续生成传感器数据
    while (running){ //标志位
      curTemp = curTemp.map(
        t => (t._1, t._2 + rand.nextGaussian())
      )
      //时间戳
      val curTime = System.currentTimeMillis()

      //每个元素foreach触发计算，使用context收集
      curTemp.foreach(
        t => sourceContext.collect( SensorReading(t._1, curTime, t._2) ) //发射出去的数据
      )

      //时间间隔
      Thread.sleep(500)
    }


  }

  //取消数据的生成
  override def cancel(): Unit = {
    running = false
  }
}
