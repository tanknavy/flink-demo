package com.tanknavy.demo

import org.apache.flink.api.scala._ //隐式转换的需要
import org.apache.flink.api.scala.ExecutionEnvironment

/**
 * Author: Alex Cheng 6/27/2020 12:50 PM
 */

//批处理word count程序，flink有DataSet批处理，DataStream流处理
object WordCount {
  def main(args: Array[String]): Unit = {
    //1.创建batch执行环境，类似spark的SparkContext
    val env = ExecutionEnvironment.getExecutionEnvironment

    //2.从文件中读取数据，DataSet api
    val inputPath = "D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\hello.txt"
    val inputDataSet = env.readTextFile(inputPath) //返回DataSet[String]

    //3.数据处理
    val wordCountDataSet = inputDataSet.flatMap(_.split(" ")) //每一行的切分成一个word
      .map((_,1)) //映射成tuple2(word,1)
      .groupBy(_._1) //也可以groupBy(0)按照第一个栏位分组，类似spark的reduceByKey，这里可以传入字段位置，字段名，或者一个提取字段的func
      .sum(1) //使用第二个字段聚合

    //4.结果输出
    wordCountDataSet.print()
  }
}
