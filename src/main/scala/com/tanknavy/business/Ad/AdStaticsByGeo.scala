package com.tanknavy.business.Ad

import java.sql.Timestamp
import java.util.Properties

import com.tanknavy.business.market.AppMarket.getClass
import com.tanknavy.business.market.{MarketCountAgg, MarketCountTotal, SimulatedEventSource}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


/**
 * Author: Alex Cheng 12/5/2020 10:20 AM
 */
//ETL后的数据，广告点击，需要考虑刷单行为(同个用户对同个广告连续点击，或者一天点击了多次，黑名单)
//考虑(user,ad)的点击量，超过了加入黑名单， 使用状态编程，超过了列入当日黑名单，定时器0点定时清理一次
//输入数据类型
case class AdClickEvent(userId: Long, adId: Long, province:String, city:String, timestamp:Long)

//输出数据类型
case class AdCountByProvince(windowEnd:String, province:String, count:Long)

//输出的黑名单报警信息
case class BlackListWarning(userId:Long, adId:Long, msg:String)

object AdStaticsByGeo {

  //定义侧输出流tag
  val blackListOutputTag: OutputTag[BlackListWarning] = new OutputTag[BlackListWarning]("blacklist")

  def main(args: Array[String]): Unit = {
    //1.环境env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //并行度
    env.setParallelism(1)

    //时间语义,event time而不是默认的process time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //指定事件time，记得还要指定如何抽取event time
    env.getConfig.setAutoWatermarkInterval(500L) //默认200ms周期产生watermark

    //开启流状态的checkpoint容错机制，间隔时间，参数设置
    env.enableCheckpointing(60 * 1000) //间隔60s,

    //选择状态后端 @deprecated Use [[StreamExecutionEnvironment.setStateBackend(StateBackend)]] instead.
    //env.setStateBackend(new MemoryStateBackend()) //有三种，状态管理和checkpoint分别在本地task jvm和远程jobManger(都内存对象), 本地 task jvm和fs(内存和文件), 本地RocksDB中(DB)
    env.setStateBackend(new FsStateBackend("file:///e:/tmp/flink/checkpoint", false)) //状态在内存对象，检查点在fs

    //2.source文件中读取
    //实际生产环境是kafka source->flink->kafka sink
    //2.1socket数据流
    //val socketStream = env.socketTextStream("localhost", 7777) //linux:nc -lk 7777， window7：nc -lp 7777

    //2.2文本文件数据源

    //val streamFromFile = env.readTextFile("D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\UserBehavior.csv")
    val resource = getClass.getResource("/AdClickLog.csv") //文件使用相对路径
    val streamFromFile = env.readTextFile(resource.getPath())

    //2.3自定义类型数据源
    val mySourceStream = env.addSource(new SimulatedEventSource()) //自定义数据源

    //2.4.配置kafka consumer client
    val props = new Properties()
    props.setProperty("bootstrap.servers", "localhost:9092")
    props.setProperty("group.id", "consumer-flink") //消费组
    props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer", "org.apache.kafka.common.serializations.StringDeserializer")
    props.setProperty("auto.offset.reset","latest") //偏移量自动重置

    //2.4.1消费kafka数据源，addSource()
    val kafkaStream = env.addSource(new FlinkKafkaConsumer011[String]("orderLog",
      new SimpleStringSchema(),props)) //kafka-topic, valueDeserializer, props


    //3.1.基本转换算子和简单聚合算子, keyBy: DataStream -> KeyedStream, 然后可以agg/reduce
    val dataStream = streamFromFile.map(
    //val dataStream = socketStream.map(
    //val dataStream = mySourceStream.map(
    ////val dataStream = kafkaStream
          data => {
            val dataArray = data.split(",")
    //        //SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //为了方便序列化写到kafka
    //        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
            AdClickEvent(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim, dataArray(3).trim, dataArray(4).trim.toLong)
          })
    //val dataStream = mySourceStream //无需map的数据源
      //timestamp和watermark 三种格式的time assigner(升序，乱序),使用event time时如果使用 time assingner产生watermark
      .assignAscendingTimestamps(_.timestamp * 1000L) //数据升序时，就不用watermark延迟触发，传入一个时间戳抽取器(毫秒)，到时间就触发不用延迟！
    //.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) { //数据乱序1，传入等待时间, wm = maxEventTs - waitTime
    //  override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
    //})
    //.assignTimestampsAndWatermarks( new MyAssigner) //数据乱序2，自定义time assigner

    //正常开窗处理前，想过滤掉广告黑名单，使用状态编程，keyBy后再process中过滤主流，还有侧输出的报警流
    val filterBackListStream = dataStream
      //.keyBy("userId", "adId") //采用位置或者名称参数，得到是Java Tuple类型[AddClickEvent, Tuple]
      .keyBy( data =>(data.userId, data.adId)) //使用函数指定Tuple,得到[AddClickEvent, (Long, Long)]，可以看到推荐！！！
      //process处理keyBy后，timeWindow前
      .process(new FilterBlackListUser(100)) //超过100次就得过滤掉，不再考虑，还需要黑名单报警sideOuput输出 //BlackListWarning(937166,1715,click over 100 times toda


    //3.2开窗,时间窗口[)左边包括，右边不包含,使用window和聚合处理，单这些不能实现业务时，使用更底层的ProcessFunction,
    // keyBy和TimeWindow顺序不一样，后面的聚合可能也不一样
    //3.2.1需求,
    //val hotPerWindowStream = dataStream //DataStream[SensorReading]
    val adCountStream = filterBackListStream //先做黑名单过滤
      //.filter( _.behavior != "UNINSTALL") //app渠道
      //.map( data =>("dummyKey", data.userId)) //使用占位符号key
      //.map(data => data.userId) //直接userId，也不keyBy了
      //.map( data => ("dummyKey", 1L) ) //Tuple2[[Tuple2], Long]
      //keyBy可以给多个字段(int位置或者string字段名都可以，但这是返回就是JavaTuple类型)
      .keyBy(_.province) //keyBy在watermark分配之后，keyBy以后开窗就是WindowedStream, 否则就是StreamAll,
      //.map(data =>(data.channel,data.behavior)) //干脆不要1L,直接WindowALl和sum，不需要process
      //几种window用法示例, keyBy以后开窗就是WindowedStream, 否则就是StreamAll
      //.windowAll(TumblingEventTimeWindows.of(Time.hours(1))) //这里只有一个key，无需keyBy
      //.timeWindowAll(Time.hours(1),Time.seconds(10))
      //.timeWindow(Time.seconds(10))//10秒的滚动窗口,是window的简写
      //.timeWindow(Time.milliseconds(20))//毫秒单位，是window的简写
      .timeWindow(Time.hours(1),Time.seconds(5))//滑动窗口,15s内，每隔5s
      //.window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5),Time.hours(-8)))//滑动窗口, 多一个8hours的offset，时区
      //.sum(1) //简单的聚合函数（比如sum对某个栏位）,不能处理复杂的业务逻辑，window time
      //.reduce( (d1,d2) => (d1._1, d1._2 + d2._2 )) //keyBy以后同分区的id一样, reduce做增量聚合，reduce后返回DataStream
      //这里除了做聚合, 还想指定输出格式, 使用aggregate,有几种形式，这里（预聚合，窗口函数），后面还要排序
      .aggregate( new AdCountAgg(), new AdCountTotal() ) //可以预聚合，除了reduce类似的增量聚合还有全窗口聚合,两个参数，一个聚合规则，一个输出数据结构
    //.keyBy(_.windowEnd)//按照每个时间窗口分区
    //.apply( new UvCountByWindow() ) //聚合操作
    //.trigger(new MyTrigger()) //不等到窗口关闭时操作聚合函数，而是主动触发关窗操作,案例用于bloom filter
    //.process(new MarketingCount()) //窗口内排序


    //业务逻辑
    //3.2.2需求: 10s秒内温度连续两次上升，window开窗和reduce都不能实现时，使用更底层的ProcessFunction,它可以访问watermark
    /* val processedStream = dataStream
       .keyBy(_.id)
       .process( new TempIncreAlert() ) //keyedProcessFunction，如果温度比上次高，则创建一个1s后的timer,如果温度下降，则删除当前timer

     //3.2.3需求: 两次温差超过threshold就报警，process是大招
     val processedStream2 = dataStream
       .keyBy(_.id)
       .process(new TempChangeAlert(10)) //只是简单比较两次温差，有必要使用process吗

     //3.2.4需求同上，温差报警，但是不用process来，使用RichFlatMapFunction,也带有状态
     val processedStream3 = dataStream
       .keyBy(_.id)
       .flatMap(new TempChangeAlert2(10)) //不使用KeyedProcessFunction,而是RichFlatMapFunction,也带有状态

     //3.3.5需求同上，温差报警，但是不用process来，使用RichFlatMapFunction,也带有状态
     //体验函数式编程， 这里对初识值处理更好
     val processedStream4 = dataStream
       .keyBy(_.id)
       //有状态flatMap, flatMapWithState(outType, stateType), 输出[TraversableOnce[R], Option[S]]类型 传入一个函数，对传入的元素
       .flatMapWithState[(String, Double, Double), Double]{ //相当于flatMap(RichFlatMapFunction),flatMap无状态，flatMapWithState有状态，reduce有状态
         //初次没有状态, 给状态赋值，返回TraversableOnce[R], Option[S]类似，这里第一次是List,和Some[]
         case (input: SensorReading, None) =>(List.empty, Some(input.temperature)) //如果没有状态，就是没有数据来过，那么就当温度值存入状态
         //有状态, 执行业务逻辑，更新转态，返回返回TraversableOnce[R], Option[S]
         case (input:SensorReading, lastTemp:Some[Double]) =>
           val diff = (input.temperature - lastTemp.get).abs
           if (diff > 10.0) {
             (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature)) //输出类型
           }else{
             (List.empty,Some(input.temperature))
           }
       }
 */
    //4. sink,使用print
    //dataStream.addSink(new MyJdbcSink())
    //dataStream.print("data stream")

    adCountStream.print("Stream Result ") //没有输出，原因1,默认时间窗口按照process time而不是event time, 原因2，窗口时间太长，还没来得及关窗户计算程序就结束了
    //侧输出流拿到黑名单告警
    filterBackListStream.getSideOutput(blackListOutputTag).print("blacklist") //传入标签

    //5. 执行
    env.execute("Ad Count")

  }

  //放在内部可以使用tag, 使用状态编程,open,close, process, onTimer
  class FilterBlackListUser(maxCount:Int) extends KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]{ //key,in,out

    //定义状态,当前是keyBy后的stream,所以考虑一个具体key中状态即可
    //keyBy后，windowStream前, 这里处理的是分组数据，相当于已经存了key，所以状态就是当前key下的状态就行了，千万不要搞复杂了，比如List，set.map这些保存状态
    lazy val countState: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("count-state",classOf[Long]))
    //如果user点击超过阈值被加入黑名单，报警一次，后面不再报警了
    lazy val isSentBlackList: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("issent-state",classOf[Boolean]))
    //0点定时器用于清空黑名单，需要保存定时器触发的时间戳
    //保存定时器触发的时间戳
    lazy val resetTimer:  ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("reset-state",classOf[Long]))

    //黑名单逻辑
    override def processElement(value: AdClickEvent, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#Context, out: Collector[AdClickEvent]): Unit = {
      //进来一条点击，更新(userId, adId)
      val currentCount = countState.value() //

      //当天某个用户第一次点击，注册一个每天零点清理一次黑名单的timer，按照这个逻辑其它用户第一次点击也会注册定时器，
      // 为了保证一个定时器，需要判断时间点定时器是否存在
      if (currentCount == 0){
        //定时器时第二天零点，今天date + 1, 能从ctx拿到当前处理系统时间(毫秒数)，先通过这个得到1970年来第几天，再乘以1000 * 60 * 60 * 24得到明天零点的时间戳
        val ts =  (ctx.timerService().currentProcessingTime() / (1000 * 60 * 60 * 24) +1) * 1000 * 60 * 60 * 24//定时器时第二天零点
        resetTimer.update(ts) //针对当前key搞个定时器
        ctx.timerService().registerProcessingTimeTimer(ts) //设置一个定时器，0点清空countState状态
      }

      //判断计数器是否达到上限
      if (currentCount >= maxCount){ //超过了需要报警一次
        //黑名单触发后只发送一次
        if(!isSentBlackList.value()){
          isSentBlackList.update(true) //保证只报警一次
          //发送黑名单到side output
          ctx.output(blackListOutputTag, BlackListWarning(value.userId,value.adId, "click over " + maxCount + " times today"))  //触发了一次报警
        }

        return //用户单日点击超过阈值，不再需要这条记录了
      }

      //计数+1，输出数据到驻留
      countState.update(currentCount + 1)
      out.collect(value) //该点击有效,放行
    }

    //定时器，0点定时清理黑名单
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[(Long, Long), AdClickEvent, AdClickEvent]#OnTimerContext, out: Collector[AdClickEvent]): Unit = {
      //新的一天，从新计数
      if (timestamp == resetTimer.value()){ //上次设定的闹钟时间
        isSentBlackList.clear()
        countState.clear()
        resetTimer.clear()
      }
    }
  }
}

//自定义预聚合函数
class AdCountAgg() extends AggregateFunction[AdClickEvent,Long,Long]{ //in, acc, out(指累加器结果)
  override def createAccumulator(): Long = 0L

  override def add(value: AdClickEvent, accumulator: Long): Long = accumulator + 1

  override def getResult(accumulator: Long): Long = accumulator

  override def merge(a: Long, b: Long): Long = a + b
}

//自定义窗口处理函数, bloom filter中在process中使用ProcessWindowFunction
class AdCountTotal() extends WindowFunction[Long, AdCountByProvince, String, TimeWindow]{ //in（上述预聚合的结果）, out, key, window
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[AdCountByProvince]): Unit = {
    //println("input元素个数:" + input.size)//经过聚合后,Iterable中之剩下最终一个数值
    out.collect(AdCountByProvince(new Timestamp(window.getEnd).toString, key, input.iterator.next() ))
  }
}

class FilterBlackListUser(){

}