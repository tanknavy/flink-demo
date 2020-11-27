package com.tanknavy.demo

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * Author: Alex Cheng 6/27/2020 1:57 PM
 */

object WordCountStream {
  def main(args: Array[String]): Unit = {
    //0.为了部署在集群中
    val params = ParameterTool.fromArgs(args) //从程序运行命令行中读取参数
    val host:String = params.get("host", "spark3")
    val port:Int = params.getInt("port", 7777)

    //1.创建stream处理的执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment //流执行环境

    //2.stream输入，接受一个socket文本流, nc -lk 7777启动netcat
    //val dataStream = env.socketTextStream("localhost",7777)
    //val dataStream = env.socketTextStream("spark3",7777)
    val dataStream = env.socketTextStream(host,port)

    //3.对输入流的每条数据进行处理
    val wordCountStream = dataStream.flatMap(_.split(" "))
      .filter(_.nonEmpty) //过滤非空
      .map((_,1))
      .keyBy(0) //流中分组,stream中没有groupBy, 流中使用keyBy, 类似spark的reduceByKey
      .sum(1) //聚合
      .setParallelism(1) //flink可以在每个算子操作设定setParallelism

    //4.实时输出
    wordCountStream.print().setParallelism(1) //设置并行度，代表执行的线程数量，默认是当前电脑的core的数量
    //输出类似 3> (hello,1), 前面的数字表示slot,意思是线程的并行度，在第几个线程上面运行, 在web界面submit时可以指定默认的，程序中指定的并行度最大

    //wordCountStream.writeAsCsv("/tmp/flink")

    //5.上面只是定义，启动executor
    env.execute("stream word count job")


  }
}
