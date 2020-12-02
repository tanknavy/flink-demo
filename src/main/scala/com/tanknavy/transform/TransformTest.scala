package com.tanknavy.transform

import com.tanknavy.source.bounded.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration

/**
 * Author: Alex Cheng 11/28/2020 3:40 PM
 */


//只有keyBy从DataStream类型到KeyedStream类型，只有KeyedStream类型才有聚合,reduce操作！
//比如就是想统计来源数据有多少个怎么办？ map时指定一个统一的占位符，然后keyBy这个占位符
//多流合并Connect和Union区别,Connect(两个流，类型可以不同，后面coMap中再去调整成一样的)和Union(可以多个流，类型必须相同)的区别
object TransformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1) //在本地开发环境中，默认并行度是core数量个
    //实际常见是kafka source->flink->kafka sink
    //socket数据流
    //val socketStream = env.socketTextStream("localhost", 7777) //linux:nc -lk 7777， window7：nc -lp 7777
    val streamFromFile = env.readTextFile("D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\sensor.txt")


    //1.基本转换算子和简单聚合算子, keyBy: DataStream -> KeyedStream, 然后可以agg/reduce
    val dataStream: DataStream[SensorReading] = streamFromFile.map(data =>{
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })

    //.keyBy(0).sum(2) //括号里面可以写int类型的索引为,string类型的case class栏位名， 还有函数式的_.name，返回泛型类型可能不一样
    //dataStream.keyBy(0).sum(2) //错误，这样另外一行，发现并没有sum
    //val stream: KeyedStream[SensorReading,String] = dataStream.keyBy(_.id) //_.id类似 fun: T => K
    .keyBy(_.id) //分组后每个元素是类似[SensorReading, keyType/Tuple]
      //.sum("temperature") //_.id类似 fun: T => K，
      //需求，当前传感器最新温度+10，而时间戳是上一次数据的时间+1,使用reduce
        .reduce( (x,y)=> SensorReading(x.id, x.timestamp+1, y.temperature+10) ) //

    //dataStream.print().setParallelism(1) //暂停输出

    //2.多流转换算子
    // split：DataStream-> SplitStream
    // select:SplitStream-> DataStream
    //比如温度传感器分成两个流(以30度分界)
    val splitStream = dataStream.split(data =>{ //流split启用，推荐使用side output
      if (data.temperature > 30) Seq("high30") else Seq("low30")
    })

    val high30 = splitStream.select("high30")
    val low30 = splitStream.select("low30")
    val all = splitStream.select("high30", "low30")

    high30.print("high30")
    low30.print("low30")
    all.print("all")
    println("-------------------------------------------------")


    //3.多流合并
    // Connect: DataStream,DataStream ->ConnectedStreams, 流数据类型可以不一样，一次两条流，同流但不合污，一国两制
    // CoMap, CoFlatMap: ConnectedStreams -> DataStream //
    // Union: DataStream, DataStream ->DataStream //包含所有DataStream的元素的新DataStream, 流数据类型必须一样，可以多条流合并
    val warningStream = high30.map( data => (data.id, data.temperature))
    val connectedStreams = warningStream.connect(low30) //两个流数据类型不一样[String, Double), SensorReading]，一个是Tuple2,一个是SensorReading, 返回ConnectedStreams

    val coMapDataStream = connectedStreams.map(//ConnectedStreams上做map就是CoMap/CoFlatMap
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, "healthy")
    )
    coMapDataStream.print("coMapDataStream") //里面的数据流类型还可以不一样

    //流数据类型一样，多流合并使用union
    val unionStream = high30.union(low30) //可以多个流
    unionStream.print("unionStream")


    //函数类，Flink暴露了所有udf函数的接口(接口或者抽象类)
    //dataStream.filter(_.id.startsWith("s_1")).print("myFilter") //匿名函数的方式,_表示输入的每个元素
    dataStream.filter(new MyFilter).print("myFilter") //没有传入函数而是使用函数类, 可以写成匿名类，或者还可以MyFilter(keyword: String)

    //Flink启动处理
    env.execute("transform test")
  }
}

//所有Flink函数类都有其函数类，富函数类
//函数类(比如Filter函数类)
class MyFilter() extends FilterFunction[SensorReading] {
  override def filter(value: SensorReading): Boolean = {
    value.id.startsWith("s_1")
  }
}

//富函数(Rich Functions)，
// 所有Flink函数类都有其Rich版本，比如可以获取运行环境的上下文，拥有一些生命周期方法，可以实现更复杂的功能
class MyMap() extends RichMapFunction[SensorReading, String]{
  //必须重写的方法
  override def map(in: SensorReading): String = {
    "Flink"
  }

  //可选重写的方法，想获取运行环境上下文
  override def open(parameters: Configuration): Unit = {
    val subTaskIndex = getRuntimeContext.getIndexOfThisSubtask //子任务编号
    //做一些初始化工作，比如建立一个和HDFS的连接
  }

  override def close(): Unit = {
    //做清理工作，比如断开和HDFS的连接
  }

}