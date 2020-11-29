package com.tanknavy.demo

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.core.fs.FileSystem
//import org.apache.flink.api.scala._
import org.apache.flink.api.scala.createTypeInformation //隐式转换函数，将输入数据转成TypeInformation[T]


/**
 * Author: Alex Cheng 6/27/2020 12:50 PM
 * 本地开发模式
 * standalone模式: 可以提交任务到UI，也可以命令行
 * >./bin/flink run -c entryClass -p 2 wordcount.jar --host localhost --port 7777
 * yarn模式
 * k8s模式
 */

//批处理word count程序，flink有DataSet批处理，DataStream流处理
object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建batch执行环境，类似spark的SparkContext
    val env = ExecutionEnvironment.getExecutionEnvironment
    //env.setParallelism(1) //在本地开发环境中，默认并行度是core数量个

    //2.从文件中读取数据，DataSet api
    val inputPath = "D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDataSet = env.readTextFile(inputPath) //返回DataSet[String]

    //3.数据处理
    val wordCountDataSet = inputDataSet.flatMap(_.split(" ")) //每一行的切分成一个word
      .map((_,1)) //映射成tuple2(word,1)
      .groupBy(0) //也可以groupBy(0)按照第一个栏位分组，类似spark的reduceByKey，这里可以传入字段位置，字段名，或者一个提取字段的func
      .sum(1) //使用第二个字段聚合

    //4.结果输出, flink里面算子可以设定并行度，默认为core的数量
    //wordCountDataSet.print()
    //想要写到文件系统，print得放到后面
    wordCountDataSet.writeAsText("e:/tmp/flink/wordcount_result",FileSystem.WriteMode.OVERWRITE).setParallelism(1)
    wordCountDataSet.print()
  }
}
