package com.tanknavy.window

import com.tanknavy.source.bounded.SensorReading
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * Author: Alex Cheng 11/30/2020 2:10 PM
 */

//时间窗口，默认是按照process time, 按照时间顺序有EventTime, IngestionTime, ProcessTime
//一般设定等待时间，watermark = maxEventTime - waitTime
object WindowTest {
  def main(args: Array[String]): Unit = {
    //1.环境env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    //event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //指定事件time，记得还要指定如何抽取event time
    env.getConfig.setAutoWatermarkInterval(500L) //默认200ms周期产生watermarkL

    //2.source文件中读取
    //实际常见是kafka source->flink->kafka sink
    //socket数据流
    val socketStream = env.socketTextStream("localhost", 7777) //nc -lk 7777

    //文本文件数据源
    val streamFromFile = env.readTextFile("D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    //自定义类型数据源
    //val mySourceStream: DataStream[SensorReading] = env.addSource(new SensorSource()) //自定义数据源
    //2.1.基本转换算子和简单聚合算子, keyBy: DataStream -> KeyedStream, 然后可以agg/reduce
    val dataStream = streamFromFile.map(
      //val dataStream = mySourceStream.map(
      //val dataStream = kafkaStream.map(
      data => {
        val dataArray = data.split(",")
        //SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //为了方便序列化写到kafka
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
      //三种格式的time assigner,使用event time时如果使用 time assingner产生watermark
      //.assignAscendingTimestamps(_.timestamp * 1000) //数据升序时，就不用watermark延迟触发，传入一个时间戳抽取器(毫秒)，到时间就触发不用延迟！
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) { //数据乱序，传入等待时间, wm = maxEventTs - waitTime
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })
      //.assignTimestampsAndWatermarks( new MyAssigner) //数据乱序，自定义time assigner

    //2.2开窗,时间窗口[)左边包括，右边不包含
    //需求：15秒滑动窗口最小温度
    val minTempPerWindowStream = dataStream
      .map( data =>(data.id,data.temperature))
      .keyBy(_._1) //keyBy在water mark分配之后，
      //.timeWindow(Time.seconds(10))//10秒的滚动窗口,是window的简写
      //.timeWindow(Time.milliseconds(20))//毫秒单位，是window的简写
      //.timeWindow(Time.seconds(15),Time.seconds(5))//滑动窗口,15s内，每隔5s
      .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5),Time.hours(-8)))//滑动窗口, 多一个8hours的offset，时区
      .reduce( (d1,d2) => (d1._1, d1._2.min(d2._2)) ) //keyBy以后同分区的id一样, reduce做增量聚合，reduce后返回DataStream



    //3. sink,使用print
    //dataStream.addSink(new MyJdbcSink())
    //dataStream.print("data stream")
    minTempPerWindowStream.print("min temp") //没有输出，原因1,默认时间窗口按照process time而不是event time, 原因2，窗口时间太长，还没来得及关窗户计算程序就结束了

    //4. 执行
    env.execute("window test")

  }

}


//自定义time assigner， 一种是Period周期性加watermark, 一种不是
//周期性watermark
class MyAssigner extends AssignerWithPeriodicWatermarks[SensorReading]{
  //定义延迟
  val bound = 60 * 1000 //60秒延迟，要结合业务，延迟过大，等待数据太慢了，可以先提前近似计算出结果，延迟过小，计算数据不准(漏掉了)，可以使用allowLateness容许等待, sideOutput侧结果输出
  var maxTs = Long.MinValue //初始化当前最大时间戳为最小

  //怎么计算watermark，默认按照process time每200ms周期调用此方法生成watermark, 可以修改
  override def getCurrentWatermark: Watermark = {
    //wm设定为 当前最大时间戳 - 延迟时间，比如当前时间戳202010秒, 延迟5秒,那么水位线就202005，相当于认为202005时间前的数据都到了，可以计算了
    new Watermark( maxTs - bound) //周期性生成时间戳watermark, 采用最大事务时间 - 延迟
  }

  //怎么抽取时间
  override def extractTimestamp(t: SensorReading, l: Long): Long = {

    //保存每个最大的event timestamp
    maxTs = maxTs.max(t.timestamp * 1000)

    //event timestamp
    t.timestamp * 1000 //毫秒单位，源数据是秒
  }
}

//非周期性的watermark生成
class MyAssigner2() extends AssignerWithPunctuatedWatermarks[SensorReading]{
  val bound = 60 * 1000 //60秒延迟，要结合业务
  override def checkAndGetNextWatermark(t: SensorReading, extractedTimestamp: Long): Watermark = {
    if (t.id == "s_1"){ //只有sensor名称是s_1时才设置watermark
      new Watermark(t.timestamp * 1000 - bound)
    }else{
      null
    }
  }

  override def extractTimestamp(t: SensorReading, previousElemTimestamp: Long): Long = t.timestamp * 1000 //需要毫秒单位
}