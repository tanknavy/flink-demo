package com.tanknavy.business.WebFlow

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties
import java.util.regex.Pattern

import com.tanknavy.business.HotItems.HotItems.getClass
import com.tanknavy.business.HotItems.{CountAgge, ItemViewCount, TopHotItem, UserBehavior, WindowResultFunction}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Author: Alex Cheng 12/3/2020 11:18 AM
 */
//解析apache log，分析网页流浪

//web log样例类
case class ApacheLogEvent(ip: String, userId:String, eventTime:Long, method: String, url:String)

//窗口聚合样例类
case class UrlViewCount(url:String, windowEnd: Long, count: Long)

object NetworkFLow {
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
    //val streamFromFile = env.readTextFile("D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\apache.log")
    val resource = getClass.getResource("UserBehavior.csv") //文件使用相对路径
    val streamFromFile = env.readTextFile(resource.getPath)

    //2.3自定义类型数据源
    //val mySourceStream: DataStream[SensorReading] = env.addSource(new SensorSource()) //自定义数据源

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
    //val dataStream = kafkaStream.map(
      data => {
        //val dataArray = data.split(",")
        val dataArray = data.split(" ")
        //SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //为了方便序列化写到kafka
        //UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
        //时间格式转换成long
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss") //按照格式解析时间
        val timestamp = simpleDateFormat.parse(dataArray(3).trim).getTime //将字符串按照格式解析，然后转millisesecond
        ApacheLogEvent(dataArray(0).trim, dataArray(1).trim, timestamp, dataArray(5).trim, dataArray(6).trim)
      })
      //watermark 三种格式的time assigner(升序，乱序),使用event time时如果使用 time assingner产生watermark
      //.assignAscendingTimestamps(_.timestamp * 1000) //数据升序时，就不用watermark延迟触发，传入一个时间戳抽取器(毫秒)，到时间就触发不用延迟！
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) { //数据乱序1，传入等待时间, wm = maxEventTs - waitTime
      override def extractTimestamp(t: ApacheLogEvent): Long = t.eventTime  //* 1000 //需要毫秒
    })
    //.assignTimestampsAndWatermarks( new MyAssigner) //数据乱序2，自定义time assigner

    //3.2开窗,时间窗口[)左边包括，右边不包含,使用window和聚合处理，单这些不能实现业务时，使用更底层的ProcessFunction,
    //3.2.1需求，热门商品top N
    val hotPerWindowStream = dataStream //DataStream[SensorReading]
      //.filter( _.behavior == "pv")
       .filter(!_.url.endsWith(".css")) //过滤掉静态资源请求,写个模式匹配吧
      .filter(!_.url.endsWith(".ico")) //过滤掉静态资源请求,写个模式匹配
      .filter(!_.url.endsWith(".png")) //过滤掉静态资源请求,写个模式匹配
      .filter(!_.url.matches("^/$")) //过滤掉静态资源请求,写个模式匹配
      //.map( data =>(data.itemId, 1))
      .keyBy(_.url) //keyBy在watermark分配之后，keyBy以后开窗就是WindowedStream, 否则就是StreamAll,
      //几种window用法示例, keyBy以后开窗就是WindowedStream, 否则就是StreamAll
      //.timeWindow(Time.seconds(10))//10秒的滚动窗口,是window的简写
      //.timeWindow(Time.milliseconds(20))//毫秒单位，是window的简写
      .timeWindow(Time.hours(1),Time.minutes(5))//滑动窗口,15s内，每隔5s
      .allowedLateness(Time.seconds(60)) //如果watermark延迟不够(想实时处理，还想处理延迟的)，可以使用sideOutput
      //.window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5),Time.hours(-8)))//滑动窗口, 多一个8hours的offset，时区
      //.reduce( (d1,d2) => (d1._1, d1._2 + d2._2 )) //keyBy以后同分区的id一样, reduce做增量聚合，reduce后返回DataStream
      //这里除了做聚合, 还想指定输出格式, 使用aggregate,有几种形式，这里（预聚合，窗口函数），后面还要排序
      .aggregate( new CountAggUrl(), new WindowResultFunctionUrl() ) //预聚合，除了reduce类似的增量聚合还有全窗口聚合,两个参数，一个聚合规则，一个输出数据结构
      .keyBy(_.windowEnd)//按照每个时间窗口分区
      //.apply()
      .process(new TopHotUrl(3)) //窗口内排序


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

    hotPerWindowStream.print("hot url ") //没有输出，原因1,默认时间窗口按照process time而不是event time, 原因2，窗口时间太长，还没来得及关窗户计算程序就结束了


    //5. 执行
    env.execute("hot url analysis")
  }
}


//aggregate聚合用,累加器, 有状态的
class CountAggUrl() extends AggregateFunction[ApacheLogEvent, Long, Long]{ //in, acc（中间聚合状态）, out
  override def createAccumulator(): Long = 0L

  override def add(in: ApacheLogEvent, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(a1: Long, a2: Long): Long =  a1 + a2 //两个累加器如果处理
}


//自定义窗口函数，计算ItemViewCount, 将CountAgge的输出包装成需要的格式
//aggregate全窗口聚合用, apply定了要输出的数据类型， 上述AggregateFunction的输出就是这里的输入
//注意:WindowFunction容易倒错包
class WindowResultFunctionUrl() extends WindowFunction[Long, UrlViewCount, String, TimeWindow]{ //in, out, key(由于是keyedStream[T,JavaTuple]), Window

  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
    //val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0 //java Tuple1,如果keyedStream中key是KeyBy("fieldName")而不是keyBy（_.fieldName）
    out.collect(UrlViewCount(key, window.getEnd, input.iterator.next())) //input表示某窗口内元素
  }
}


//自定义预聚合函数计算平均值, (sum, count)作为一个tuple
class AverageAgg() extends AggregateFunction[UserBehavior, (Long, Int), Double]{
  override def createAccumulator(): (Long, Int) = (0L,0)

  override def add(in: UserBehavior, acc: (Long, Int)): (Long, Int) = (in.timestamp + acc._1, acc._2 + 1)

  override def getResult(acc: (Long, Int)): Double = acc._1 / acc._2

  override def merge(a1: (Long, Int), a2: (Long, Int)): (Long, Int) = (a1._1 + a2._1, a1._2 + a2._2)
}


//keyedStream， 分组后liststate保存每个窗口全部元素作为状态， 按照windowEnd时间注册定时器，定时器中对liststate排序求topN
class TopHotUrl(topN: Int) extends KeyedProcessFunction[Long, UrlViewCount, String] { //key,in,out

  //状态定义和初始化方法一，在生命周期open中
  private var itemState: ListState[UrlViewCount] = _
  //状态定义和初始化方法二，lazy懒加载，需要时再定义
  //lazy val itemState2: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("item-states", classOf[UrlViewCount]))

  //key, in, out
  override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, collector: Collector[String]): Unit = {
    //把每条数据存入状态列表，窗口关闭时排序取topN,为啥不用TreeMap,
    itemState.add(value) //追加一个元素
    //注册一个定时器，
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100) //每个item带有窗口结束时间，加个延时，确保数据都到
  }

  override def open(parameters: Configuration): Unit = {
    itemState = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("url-states", classOf[UrlViewCount]))
  }

  override def close(): Unit = itemState.clear()

  //定时器触发后, 排序并输出topN
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //所有state列表的数据取出, topN放到list buffer中
    val allItems: ListBuffer[UrlViewCount] = new ListBuffer()
    //import scala.collection.JavaConversions._ //java,scala集合类型相互转换
    val iter = itemState.get().iterator() //使用迭代器
    while (iter.hasNext){
      allItems += iter.next()
    }
    //按照count大小排序，并格式化
    //val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topN) //sortBy按照那个元素排序
    val sortedItems = allItems.sortWith(_.count > _.count).take(topN) //sortWith排序，第一个元素比第二个大
    //val sortedItems = allItems.sorted(Ordering.by(_.count)) //sorted排序失败

    //清空状态
    //输出结果
    val result: StringBuilder = new StringBuilder()
    result.append("时间: ").append(new Timestamp(timestamp - 100)).append("\n") //这个timestamp在watermark + 1后的，实际windowsEnd小一
    for (i <- sortedItems.indices) {
      val currentItem = sortedItems(i)
      result.append("No.").append(i + 1).append(":").append(" Url=").append(currentItem.url).append(" 数量=").append(currentItem.count).append("\n")
    }
    println("=====================================")
    Thread.sleep(1000) //为了看清楚
    out.collect(result.toString()) //正真输出
  }


}