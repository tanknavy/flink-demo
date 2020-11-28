package com.tanknavy.source.bounded

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment //隐式转换函数，将输入数据转成TypeInformation[T]

/**
 * Author: Alex Cheng 11/28/2020 11:02 AM
 */

//温度传感器样例类
case class SensorReading(id: String, timestamp: Long, temperature: Double)

//flink源测试
object SourceTest {
  def main(args: Array[String]): Unit = {

    //1.创建环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //2.1从自定义集合中读取数据
    val stream1 = env.fromCollection(List(
      SensorReading("s_1", 1547718199, 35.800),
      SensorReading("s_2", 1547718201, 15.442),
      SensorReading("s_3", 1547718203, 6.720),
      SensorReading("s_4", 1547718205, 35.106)
    ))

    //2.2从文件中读取，类似批处理，相当于处理有界的流
    val stream2 = env.readTextFile("D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\sensor.txt")


    //2.3从元素中
    val stream3 = env.fromElements(1,3.14,"string").print("stream3").setParallelism(1)


    stream1.print("stream1").setParallelism(1)
    stream2.print("stream2").setParallelism(1)
    env.execute("SourceTest")
  }
}
