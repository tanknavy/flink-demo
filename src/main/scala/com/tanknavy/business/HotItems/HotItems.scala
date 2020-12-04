package com.tanknavy.business.HotItems

import java.sql.Timestamp
import java.util.Properties

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Author: Alex Cheng 12/2/2020 6:52 PM
 */
//user behavior analysis, csv file
case class UserBehavior(userId:Long, itemId:Long, categoryId: Int, behavior:String, timestamp:Long)

//输出结果（商品id, 窗口结束时间，个数）
case class ItemViewCount(itemId:Long, windowEnd:Long, count:Long)

//窗口聚合结果样例类，比如写入redis可以实时被visualization
//item分组聚合(数据加上个窗口时间)->窗口时间分组后再按照count排序求topN（在Process中使用onTimer时间服务，一批都到了再排序topN）
object HotItems {
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
    val resource = getClass.getResource("/UserBehavior.csv") //文件使用相对路径
    val streamFromFIle = env.readTextFile(resource.getPath)

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
    //val dataStream = streamFromFile.map(
    //val dataStream = socketStream.map(
      //val dataStream = mySourceStream.map(
      val dataStream = kafkaStream.map(
      data => {
        val dataArray = data.split(",")
        //SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //为了方便序列化写到kafka
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      //watermark 三种格式的time assigner(升序，乱序),使用event time时如果使用 time assingner产生watermark
      .assignAscendingTimestamps(_.timestamp * 1000) //数据升序时，就不用watermark延迟触发，传入一个时间戳抽取器(毫秒)，到时间就触发不用延迟！
      //.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) { //数据乱序1，传入等待时间, wm = maxEventTs - waitTime
      //  override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      //})
      //.assignTimestampsAndWatermarks( new MyAssigner) //数据乱序2，自定义time assigner

    //3.2开窗,时间窗口[)左边包括，右边不包含,使用window和聚合处理，单这些不能实现业务时，使用更底层的ProcessFunction,
    //3.2.1需求，热门商品top N
    val hotPerWindowStream = dataStream //DataStream[SensorReading]
        //.filter( _.behavior == "pv")
      //.map( data =>(data.itemId, 1))
      .keyBy(_.itemId) //keyBy在watermark分配之后，keyBy以后开窗就是WindowedStream, 否则就是StreamAll,
      //几种window用法示例, keyBy以后开窗就是WindowedStream, 否则就是StreamAll
      //.timeWindow(Time.seconds(10))//10秒的滚动窗口,是window的简写
      //.timeWindow(Time.milliseconds(20))//毫秒单位，是window的简写
      .timeWindow(Time.hours(1),Time.minutes(5))//滑动窗口,15s内，每隔5s
      //.window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5),Time.hours(-8)))//滑动窗口, 多一个8hours的offset，时区
      //.reduce( (d1,d2) => (d1._1, d1._2 + d2._2 )) //keyBy以后同分区的id一样, reduce做增量聚合，reduce后返回DataStream
      //这里除了做聚合, 还想指定输出格式, 使用aggregate,有几种形式，这里（预聚合，窗口函数），后面还要排序
      .aggregate( new CountAgge(), new WindowResultFunction() ) //除了reduce类似的增量聚合还有全窗口聚合,两个参数，一个聚合规则，一个输出数据结构
        .keyBy(_.windowEnd)//按照每个时间窗口分区
      //.apply()
      .process(new TopHotItem(3)) //窗口内排序

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

    hotPerWindowStream.print("hot item ") //没有输出，原因1,默认时间窗口按照process time而不是event time, 原因2，窗口时间太长，还没来得及关窗户计算程序就结束了
//    processedStream.print("alert incre in 10s") //10秒内温度连续上升则报警，timer实现10秒后callback
//    //温差太大报警，使用KeyedProcessFunction
//    processedStream2.print("alert for big diff with process")
//    //温差太大报警，使用RichFlatMapFunction
//    processedStream3.print("alert for big diff with flatMap")
//    //温差太大报警，使用FlatMapWithStatus, 直接使用函数式编程
//    processedStream4.print("alert for big diff with flatMapWithState")


    //5. 执行
    env.execute("hot item analysis")
  }
}

//aggregate聚合用,累加器, 有状态的
class CountAgge() extends AggregateFunction[UserBehavior, Long, Long]{ //in, acc（中间聚合状态）, out
  override def createAccumulator(): Long = 0L

  override def add(in: UserBehavior, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(a1: Long, a2: Long): Long =  a1 + a2 //两个累加器如果处理
}


//自定义窗口函数，计算ItemViewCount, 将CountAgge的输出包装成需要的格式
//aggregate全窗口聚合用, apply定了要输出的数据类型， 上述的输出就是这里的输入
class WindowResultFunction() extends WindowFunction[Long, ItemViewCount, Long, TimeWindow]{ //in, out, key(由于是keyedStream[T,JavaTuple]), Window

  override def apply(key: Long, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    //val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0 //java Tuple1,如果keyedStream中key是KeyBy("fieldName")而不是keyBy（_.fieldName）
    out.collect(ItemViewCount(key, window.getEnd, input.iterator.next())) //input表示某窗口内元素
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
class TopHotItem(topN: Int) extends KeyedProcessFunction[Long, ItemViewCount, String]{

  //状态定义和初始化方法一，在生命周期open中
  private var itemState: ListState[ItemViewCount] = _
  //状态定义和初始化方法二，lazy懒加载，需要时再定义
  //lazy val itemState2: ListState[ItemViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-states", classOf[ItemViewCount]))

  //key, in, out,
  override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {
    //把每条数据存入状态列表，窗口关闭时排序取topN,为啥不用TreeMap,
    itemState.add(value) //追加一个元素
    //注册一个定时器，
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 100) //每个item带有窗口结束时间，加个延时，确保数据都到
  }

  override def open(parameters: Configuration): Unit = {
      itemState = getRuntimeContext.getListState(new ListStateDescriptor[ItemViewCount]("item-states", classOf[ItemViewCount]))
  }

  override def close(): Unit = itemState.clear()

  //定时器触发后, 排序并输出topN
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
    //所有state列表的数据取出, topN放到list buffer中
    val allItems: ListBuffer[ItemViewCount] = new ListBuffer()
    import scala.collection.JavaConversions._ //java,scala集合类型相互转换
    for(item <- itemState.get()){ //java结合到scala集合
      allItems += item
    }
    //按照count大小排序，并格式化
    val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topN)

    //清空状态
    //输出结果
    val result: StringBuilder = new StringBuilder()
    result.append("时间: ").append(new Timestamp(timestamp -100)).append("\n") //这个timestamp在watermark + 1后的，实际windowsEnd小一
    for( i <- sortedItems.indices){
      val currentItem = sortedItems(i)
      result.append("No.").append(i+1).append(":").append(" 商品Id=").append(currentItem.itemId).append(" 数量=").append(currentItem.count).append("\n")
    }
    println("=====================================")
    Thread.sleep(1000) //为了看清楚
    out.collect(result.toString())
  }
}