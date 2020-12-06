package com.tanknavy.business.HotItems

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

import scala.util.Random

/**
 * Author: Alex Cheng 12/3/2020 8:57 PM
 */

//之前set都在flink内存，关窗时再计算set size,速度快但数据量极大时内存消耗大
//现在来一个处理一个(bitmap和windowEnd:Count在redis中)，每个要和redis读写更新, 空间小了，但是速度慢了
//考虑不要来一个trigger对一个的处理，而是适当使用flink空间，来一批，平衡空间和时间
object UserViewBoomFilterBatch {
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
    val streamFromFile = env.readTextFile(resource.getPath())

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
      //val dataStream = kafkaStream
      data => {
        val dataArray = data.split(",")
        //SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //为了方便序列化写到kafka
        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
      })
      //watermark 三种格式的time assigner(升序，乱序),使用event time时如果使用 time assingner产生watermark
      .assignAscendingTimestamps(_.timestamp * 1000L) //数据升序时，就不用watermark延迟触发，传入一个时间戳抽取器(毫秒)，到时间就触发不用延迟！
    //.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) { //数据乱序1，传入等待时间, wm = maxEventTs - waitTime
    //  override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
    //})
    //.assignTimestampsAndWatermarks( new MyAssigner) //数据乱序2，自定义time assigner

    //3.2开窗,时间窗口[)左边包括，右边不包含,使用window和聚合处理，单这些不能实现业务时，使用更底层的ProcessFunction,
    //3.2.1需求，热门商品top N
    val hotPerWindowStream = dataStream //DataStream[SensorReading]
      .filter( _.behavior == "pv")
      .map( data =>("dummyKey", data.userId)) //使用占位符号key
      //.map(data => data.userId) //直接userId，也不keyBy了
      .keyBy(_._1) //keyBy在watermark分配之后，keyBy以后开窗就是WindowedStream, 否则就是StreamAll,
      //几种window用法示例, keyBy以后开窗就是WindowedStream, 否则就是StreamAll
      //.windowAll(TumblingEventTimeWindows.of(Time.hours(1))) //这里只有一个key，无需keyBy
      //.timeWindowAll(Time.hours(1))
      //.timeWindow(Time.seconds(10))//10秒的滚动窗口,是window的简写
      //.timeWindow(Time.milliseconds(20))//毫秒单位，是window的简写
      .timeWindow(Time.hours(1))//滑动窗口,15s内，每隔5s
      //.window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5),Time.hours(-8)))//滑动窗口, 多一个8hours的offset，时区
      //.sum(1) //简单的聚合函数
      //.reduce( (d1,d2) => (d1._1, d1._2 + d2._2 )) //keyBy以后同分区的id一样, reduce做增量聚合，reduce后返回DataStream
      //这里除了做聚合, 还想指定输出格式, 使用aggregate,有几种形式，这里（预聚合，窗口函数），后面还要排序
      //.aggregate( new CountAgge(), new WindowResultFunction() ) //除了reduce类似的增量聚合还有全窗口聚合,两个参数，一个聚合规则，一个输出数据结构
      //.keyBy(_.windowEnd)//按照每个时间窗口分区
      //.apply( new UvCountByWindow() ) //聚合操作
        .trigger(new MyTriggerBatch()) //不等到窗口关闭时操作聚合函数，而是主动触发关窗操作
      .process(new UvCountWithBloomBatch()) //窗口内排序


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

    hotPerWindowStream.print("UV ") //没有输出，原因1,默认时间窗口按照process time而不是event time, 原因2，窗口时间太长，还没来得及关窗户计算程序就结束了


    //5. 执行
    env.execute("UV analysis")
  }

}



//触发窗口的计算操作，而不是等窗口关闭，结合布隆过滤器，来一个元素查重并更新count,然后清理该元素
// TriggerResult.FIRE_AND_PURGE这样flink不用一个window内存储全部元素而是来一个计算一个
class MyTriggerBatch() extends  Trigger[(String,Long), TimeWindow]{ //input(dummyKey, userId), window
  val batchSize = 10
  var index = 0//发现这个trigger不是每个窗口初始化的，而是整个公用,我需要的是by Window内攒一小批
  //val random = new Random()
  //random.nextInt(10)
  override def onElement(t: (String, Long), l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = {
    //每来一条数据，就触发窗口操作，并清空窗口状态
    //TriggerResult.FIRE_AND_PURGE
    //减少每个数据都读写redis，考虑比如攒到一批100个再触发一次？
    if (index % batchSize == 0){ //攒一批处理一次
      index += 1
      println("index: " + index + ",测试一次处理一批头")
      TriggerResult.FIRE_AND_PURGE
    }else{
      //println("测试一次处理一批其它")
      index += 1
      TriggerResult.CONTINUE //继续攒
    }

  }

  override def onProcessingTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(l: Long, w: TimeWindow, triggerContext: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def clear(w: TimeWindow, triggerContext: Trigger.TriggerContext): Unit = {index = 0}
}

//布隆过滤器，bitmap多大
class Bloom2(size: Long) { //int就可以代表20亿
 //位图大小
  private val cap = if(size >0) size else  1 << 27 //<<左移位，结果16M byte

  //位图的核心，hash函数
  def hash(key:String, seed:Int):Long = { //key,随机种子质数，减少碰撞
    var result:Long = 0L //保存结果
    for(i <- 0 until  key.length){
      result = result * seed + key.charAt(i) // 从左到右每个字符 * seed ,
    }
    //result //长整型，因为bitmap设定为27位
    result & (cap -1) //逻辑与预算，cap是第一位是1，后面都是0， -1以后前面是0，后面26位是1， 逻辑与就一定在27个1之间
  }
}

//每来一个数据就要和redis拿当前窗口的bitmap比对，并更新count
// >hgetall UvCount
class UvCountWithBloomBatch() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow]{ //in, out, key, window
//class UvCountWithBloom() extends ProcessAllWindowFunction[Long, UvCount, TimeWindow]{ //in, out, window, 没有keyBy就没有key
  //定义redis连接
  lazy val jedis = new Jedis("localhost",6379) //jeids client
  lazy val bloom = new Bloom(1<<29) //512m bit= 64Mb //能处理5亿个， 一个窗口一个过滤器

  //override def process(ctx: ProcessAllWindowFunction[Long, UvCount, TimeWindow]#Context, iterable: lang.Iterable[Long], collector: Collector[UvCount]): Unit = {
  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
  //def process(ctx: ProcessAllWindowFunction[(String, Long), UvCount, TimeWindow]#Context, iterable: lang.Iterable[(String, Long)], collector: Collector[UvCount]): Unit = {
    //位图的存储方式，每个窗口使用一个bitmap左判断, key是WindowEnd, value是bitmap
    val storeKey = context.window.getEnd().toString //当前窗口结束时间
    var count = 0L //当前窗口count值
    //把每个窗口的uv count值存入redis，hash形式存入，表名UvCount, 里面是windowEnd：count格式
    //先从redis中，再判断当前key是否存在
    if(jedis.hget("UvCount", storeKey) !=null){
      count = jedis.hget("UvCount", storeKey).toLong //storeKey是WindowEnd
    }

    //布隆过滤器判断是否存在，process中参数iterable是当前窗口目前的元素集合，但是窗口做了trigger，TriggerResult.FIRE_AND_PURGE,来一条处理一条并清理
    //结合trigger逐个处理时，无需以下的for循环
    //val userId = elements.last._2.toString //当前窗口的元素集合最后一个元素(dummyKey, userId)，一次处理一个元素，效率有点低呀

    //结合trigger批次处理时，增加循环判断，最后一点不足batchSize的会在关窗时被处理吗,重复处理
    val userIdBatch: Iterable[(String, Long)] = elements.take(10) //trigger.FIRE_AND_PURGE中一次攒100个，这个一次处理100个，
    println("当前窗口内元素个数：" +  elements.size + ",当前批次内元素个数：" + userIdBatch.size)
    for( item <- userIdBatch){
      if(jedis.hget("UvCount", storeKey) !=null){
        count = jedis.hget("UvCount", storeKey).toLong //storeKey是WindowEnd
      }
      //逐个处理
      val userId = item._2.toString //当前窗口的元素集合最后一个元素(dummyKey, userId)，一次处理一个元素，效率有点低呀
      //println(item)
      val offset = bloom.hash(userId, 34) //userId哈希，结果是29位二进制内的
      //定义一个标识位，判断redis位置是否有这一个位
      //https://redis.io/commands/getbit
      val isExist = jedis.getbit(storeKey,offset) //redis的getbit对storeKey(windowEnd)这个可以下面的标志位计算是否为ture,实现bloom过滤器
      println(isExist)
      if (!isExist){ //如果不存在这个userId

        //如果不存在,对应位置置为1
        jedis.setbit(storeKey, offset, true) //bitmap该对象的位置(offset)为1
        jedis.hset("UvCount", storeKey, (count +1 ).toString) //count +1
        //批量时，count也没有更新
        out.collect(UvCount(storeKey.toLong, count+1))
      }else{//如果存在，不需要更新数据
        out.collect(UvCount(storeKey.toLong, count)) //只打印，不更新数据
      }

    }



  }

}

