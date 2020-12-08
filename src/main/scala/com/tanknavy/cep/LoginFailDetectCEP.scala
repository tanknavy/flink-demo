package com.tanknavy.cep

import java.util
import java.util.Properties

import com.tanknavy.business.market.SimulatedEventSource
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
 * Author: Alex Cheng 12/5/2020 3:54 PM
 */
//复杂事件处理(Complex Event Processing, CEP),在无休止的事件流中检测事件模式，事件流通过一定的规则匹配，输出用户想得到的数据
//处理事件的规则，叫做模式(Pattern）,Flink CEP提供了patter API

//账户连续登陆失败检车
//case class LoginEvent(userId:Long, ip:String, eventType:String,  timestamp:Long)
//case class LoginWarning(userId:Long, firstFailTime:Long, lastFailTime:Long, warningMsg: String)

object LoginFailDetectCEP {
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

    //val streamFromFile = env.readTextFile("D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\LoginLog.csv")
    val resource = getClass.getResource("/LoginLog.csv") //文件使用相对路径
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
    //      val dataStream = mySourceStream.map(
    //      //val dataStream = kafkaStream
          data => {
            val dataArray = data.split(",")
    //        //SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //为了方便序列化写到kafka
    //        UserBehavior(dataArray(0).trim.toLong, dataArray(1).trim.toLong, dataArray(2).trim.toInt, dataArray(3).trim, dataArray(4).trim.toLong)
              LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
          })
    //val dataStream = mySourceStream //无需map的数据源
      //watermark 三种格式的time assigner(升序，乱序),使用event time时如果使用 time assingner产生watermark
      //.assignAscendingTimestamps(_.timestamp) //数据升序时，就不用watermark延迟触发，传入一个时间戳抽取器(毫秒)，到时间就触发不用延迟！
    .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) { //数据乱序1，传入等待时间, wm = maxEventTs - waitTime
      override def extractTimestamp(t: LoginEvent): Long = t.timestamp * 1000
    })
    //.assignTimestampsAndWatermarks( new MyAssigner) //数据乱序2，自定义time assigner

    //3.2开窗,时间窗口[)左边包括，右边不包含,使用window和聚合处理，单这些不能实现业务时，使用更底层的ProcessFunction,
    // keyBy和TimeWindow顺序不一样，后面的聚合可能也不一样
    //3.2.1需求,
    val loginEventStream = dataStream //DataStream[SensorReading]
      //.filter( _.behavior != "UNINSTALL") //app渠道
      //.map( data =>("dummyKey", data.userId)) //使用占位符号key
      //.map(data => data.userId) //直接userId，也不keyBy了
      //.map( data => ("dummyKey", 1L) ) //Tuple2[[Tuple2], Long]
      //用户登录连续失败检测
      .keyBy(_.userId) //keyBy在watermark分配之后，keyBy以后开窗就是WindowedStream, 否则就是StreamAll,
      //.map(data =>(data.channel,data.behavior)) //干脆不要1L,直接WindowALl和sum，不需要process
      //几种window用法示例, keyBy以后开窗就是WindowedStream, 否则就是StreamAll
      //.windowAll(TumblingEventTimeWindows.of(Time.hours(1))) //这里只有一个key，无需keyBy
      //.timeWindowAll(Time.hours(1),Time.seconds(10))
      //.timeWindow(Time.seconds(10))//10秒的滚动窗口,是window的简写
      //.timeWindow(Time.milliseconds(20))//毫秒单位，是window的简写
      //.timeWindow(Time.hours(1),Time.seconds(10))//滑动窗口,15s内，每隔5s
      //.window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5),Time.hours(-8)))//滑动窗口, 多一个8hours的offset，时区
      //.sum(1) //简单的聚合函数,不能处理复杂的业务逻辑，window time
      //.reduce( (d1,d2) => (d1._1, d1._2 + d2._2 )) //keyBy以后同分区的id一样, reduce做增量聚合，reduce后返回DataStream
      //这里除了做聚合, 还想指定输出格式, 使用aggregate,有几种形式，这里（预聚合，窗口函数），后面还要排序
      //.aggregate( new MarketCountAgg(), new MarketCountTotal() ) //可以预聚合，除了reduce类似的增量聚合还有全窗口聚合,两个参数，一个聚合规则，一个输出数据结构
    //.keyBy(_.windowEnd)//按照每个时间窗口分区
    //.apply( new UvCountByWindow() ) //聚合操作
    //.trigger(new MyTrigger()) //不等到窗口关闭时操作聚合函数，而是主动触发关窗操作,案例用于bloom filter
    //.process(new LoginWarningProcess(2,2)) //2秒内连续失败2次, 希望能更实时，2秒内超过n就开始报警，而不是一定等2s
     // .process(new LoginWarningProcess2(2,2)) //更实时，2秒内超过n就开始报警，而不是一定等2s，但只能2次

    //使用CEP库,定义匹配模式
    //两秒之内连续两次失败模式，如果多次呢,start后使用times(3)指定多次，还有optional, greedy,
    // 疑问：能处理乱序的吗？ 看watermark
    val loginFailPattern  = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
        .next("second").where(_.eventType =="fail") //next严格近邻,followedBy宽松近邻，followedByAny非确定性宽松近邻(之前匹配过的也可以再次使用)
        .within(Time.seconds(2))
    //在事件流上应用pattern,得到一个pattern stream
    val patterStream = CEP.pattern(loginEventStream, loginFailPattern)

    //从pattern stream上应用select function, 检出匹配的事件薛烈
    val loginFailDataStream = patterStream.select(new LoginFailMatch()) //连续两次失败，所以有两种事件


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

    //hotPerWindowStream.print("UV ") //没有输出，原因1,默认时间窗口按照process time而不是event time, 原因2，窗口时间太长，还没来得及关窗户计算程序就结束了
    loginFailDataStream.print("login fail")

    //5. 执行
    env.execute("UV analysis")
  }
}


//CEP模式匹配后选择, 第一次fail和第二次fail
class LoginFailMatch() extends  PatternSelectFunction[LoginEvent, LoginWarning]{ //in,out
  override def select(map: util.Map[String, util.List[LoginEvent]]): LoginWarning = { //检测到所有的事件序列，这里有begin和next两次fail事件
    //从map中按照名称取出对应的事件
    val firstFail = map.get("begin").iterator().next() //满足条件的第一次事假
    val secondFail = map.get("second").iterator().next()
    LoginWarning(firstFail.userId, firstFail.timestamp, secondFail.timestamp, "login fail 2 times within 2 second")
  }
}