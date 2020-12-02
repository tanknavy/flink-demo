package com.tanknavy.window

import com.tanknavy.source.bounded.SensorReading
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * Author: Alex Cheng 11/30/2020 10:31 PM
 */

//Side output取代split, 它可以输出不同类型数据到一个或多个流
//还有类似的CoProcessFunction，类似CoMap
object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1) //在本地开发环境中，默认并行度是core数量个

    //实际常见是kafka source->flink->kafka sink
    //socket数据流
    val socketStream = env.socketTextStream("localhost", 7777) //linux:nc -lk 7777， window7：nc -lp 7777
    //val streamFromFile = env.readTextFile("D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\sensor.txt")


    //1.基本转换算子和简单聚合算子, keyBy: DataStream -> KeyedStream, 然后可以agg/reduce
    val dataStream: DataStream[SensorReading] = socketStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
      //三种格式的time assigner,使用event time时如果使用 time assingner产生watermark
      //.assignAscendingTimestamps(_.timestamp * 1000) //数据升序时，就不用watermark延迟触发，传入一个时间戳抽取器(毫秒)，到时间就触发不用延迟！
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) { //数据乱序，传入等待时间, wm = maxEventTs - waitTime
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })
    //.assignTimestampsAndWatermarks( new MyAssigner) //数据乱序，自定义time assigner


    //.keyBy(0).sum(2) //括号里面可以写int类型的索引为,string类型的case class栏位名， 还有函数式的_.name，返回泛型类型可能不一样
    //dataStream.keyBy(0).sum(2) //错误，这样另外一行，发现并没有sum
    //val stream: KeyedStream[SensorReading,String] = dataStream.keyBy(_.id) //_.id类似 fun: T => K
    //.keyBy(_.id) //分组后每个元素是类似[SensorReading, keyType/Tuple]
    //.sum("temperature") //_.id类似 fun: T => K，
    //需求，当前传感器最新温度+10，而时间戳是上一次数据的时间+1,使用reduce
    //.reduce( (x,y)=> SensorReading(x.id, x.timestamp+1, y.temperature+10) ) //

    //dataStream.print().setParallelism(1) //暂停输出

    //2.多流转换算子
    // split：DataStream-> SplitStream
    // select:SplitStream-> DataStream
    //比如温度传感器分成两个流(以30度分界)
/*    val splitStream = dataStream.split(data => { //流split启用，推荐使用side output
      if (data.temperature > 30) Seq("high30") else Seq("low30")
    })
    val high30 = splitStream.select("high30")
    val low30 = splitStream.select("low30")
    val all = splitStream.select("high30", "low30")

    high30.print("high30")
    low30.print("low30")
    all.print("all")*/

    val processedStream = dataStream.process(new FreezingAlert) //使用侧输出流

    processedStream.print("main stream")
    processedStream.getSideOutput(new OutputTag[String]("freezing-alert")).print("side stream") //侧输出流


    //Flink启动处理
    env.execute("transform test")
  }
}

//冰点报警，如果小于32F，输出报警信息到侧输出流
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading]{ //in, out, 主输出流

  //如果不使用lazy,在java里面可能是组合一个属性，在使用时constructor中传入这个参数
  lazy val alertOutput: OutputTag[String] = new OutputTag[String]("freezing-alert")

  override def processElement(value: SensorReading, ctx: ProcessFunction[SensorReading, SensorReading]#Context, out: Collector[SensorReading]): Unit = {
    if (value.temperature < 32.0){ //侧输出流
      //输入输出到侧输出流？ 使用context
      ctx.output(alertOutput, "freezing alert for " + value.id)
    }else{ //主输出流
      out.collect(value)
    }
  }
}
