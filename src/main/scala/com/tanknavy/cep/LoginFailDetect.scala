package com.tanknavy.cep

import java.util.Properties

import com.tanknavy.business.market.AppMarket.getClass
import com.tanknavy.business.market.{MarketCountAgg, MarketCountTotal, SimulatedEventSource}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.{KeyedProcessFunction, ProcessFunction}
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

/**
 * Author: Alex Cheng 12/5/2020 3:54 PM
 */
//账户连续登陆失败检车
case class LoginEvent(userId:Long, ip:String, eventType:String,  timestamp:Long)
case class LoginWarning(userId:Long, firstFailTime:Long, lastFailTime:Long, warningMsg: String)

object LoginFailDetect {
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
    val hotPerWindowStream = dataStream //DataStream[SensorReading]
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
      .process(new LoginWarningProcess2(2,2)) //更实时，2秒内超过n就开始报警，而不是一定等2s，但只能2次

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

//处理Keyed Stream, 实现KeyedProcessFunction
//状态编程，2秒内连续登录失败几次，就触发警告，使用onTimer定时器
//bug: 基于onTimer的fail/fail/fail/ok不会报警(最后一个ok清空了)， 2秒内大量fail但还 fail/ok/fail/fail会报警(后面2次触发是要2秒后报警)
//希望2秒内，只要超过阈值就报警
class LoginWarningProcess(timeGapSeconds:Int, maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginWarning]{ //key, in, out
  //定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-state",classOf[LoginEvent]))
  //定时器时间戳,不用，因为要在onTimer中再判断是否报警

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginWarning]#Context, out: Collector[LoginWarning]): Unit = {
    val loginFailList = loginFailState.get() //返回是Iterable类型

    //来一个事件判断一个状态，只添加fail的事件
    if(value.eventType == "fail"){//如果这次登陆失败,并且loginFaileState是初始值，那么可以注册一个定时器了
      if(!loginFailList.iterator().hasNext){//如果没有值，第一次失败注册一个定时器，用于触发
        ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + timeGapSeconds * 1000L) //几秒钟后触发,定时器单位毫秒
      }
      loginFailState.add(value)
    }else { //如果这次登陆成功清空状态, bug:报警)，fail/fail/fail/ok不会报警(最后一个ok清空了)， 2秒内大量fail但还 fail/ok/fail/fail会报警(后面2次触发是要2秒后报警
      loginFailState.clear()
    }

  }

  //定时器触发时操作
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginWarning]#OnTimerContext, out: Collector[LoginWarning]): Unit = {
    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]
    val iter = loginFailState.get().iterator() //迭代器
    while(iter.hasNext){
      allLoginFails += iter.next()
    }

    //连续失败几次了, ctx可以拿到信息
    if(allLoginFails.length >= maxFailTimes){
      //主输出流
      out.collect(LoginWarning(allLoginFails.head.userId, allLoginFails.head.timestamp, allLoginFails.last.timestamp,
        "login failed " + maxFailTimes + " in " + timeGapSeconds + " s!"))
    }
    //清空状态
    loginFailState.clear()

  }
}


//优化上述问题2秒2次登陆失败，这次失败和上次失败时间戳比较(只能判断连续2次)，如果2秒内失败则报警，但是如果要求2秒失败3次以上呢？
//bug: 只能2次, 不能处理乱序时间
class LoginWarningProcess2(timeGapSeconds:Int, maxFailTimes: Int) extends KeyedProcessFunction[Long, LoginEvent, LoginWarning]{ //key, in, out
  //定义状态，保存2秒内的所有登录失败事件
  lazy val loginFailState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("login-state",classOf[LoginEvent]))
  //定时器时间戳

  override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, LoginWarning]#Context, out: Collector[LoginWarning]): Unit = {
//    val loginFailList = loginFailState.get() //返回是Iterable类型
//
//    //来一个事件判断一个状态，只添加fail的事件
//    if(value.eventType == "fail"){//如果这次登陆失败,并且loginFaileState是初始值，那么可以注册一个定时器了
//      if(!loginFailList.iterator().hasNext){//如果没有值，第一次失败注册一个定时器，用于触发
//        ctx.timerService().registerEventTimeTimer(value.timestamp * 1000L + timeGapSeconds * 1000L) //几秒钟后触发,定时器单位毫秒
//      }
//      loginFailState.add(value)
//    }else { //如果这次登陆成功清空状态, bug:报警)，fail/fail/fail/ok不会报警(最后一个ok清空了)， 2秒内大量fail但还 fail/ok/fail/fail会报警(后面2次触发是要2秒后报警
//      loginFailState.clear()
//    }

    if(value.eventType == "fail"){
      //如果是失败，判断之前是否有登陆失败事件
      val iter = loginFailState.get().iterator()
      if(iter.hasNext){ //如果已经有登陆失败事件，就比较事件时间，注:这个只适合检查连续2次失败，因为它只比较最近两个
        val firstFail = iter.next()
        if(value.timestamp < firstFail.timestamp + timeGapSeconds){ //n秒失败2次，如果时间乱序呢？这种判断不行!!!,考虑使用onTimer利用watermark+延时
          //如果两次间隔小于2秒，输出报警
          out.collect(LoginWarning(value.userId, firstFail.timestamp, value.timestamp, "login failed 2 times in 2s"))
        }
        //更新为最近一次fail,保存在状态里
        loginFailState.clear()
        loginFailState.add(value)
      }else{ //第一次登陆失败
        loginFailState.add(value)
      }
    }else{ //成功就清空状态
      loginFailState.clear()
    }

  }

  //定时器触发时操作
/*  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, LoginWarning]#OnTimerContext, out: Collector[LoginWarning]): Unit = {
    val allLoginFails: ListBuffer[LoginEvent] = new ListBuffer[LoginEvent]
    val iter = loginFailState.get().iterator() //迭代器
    while(iter.hasNext){
      allLoginFails += iter.next()
    }

    //连续失败几次了, ctx可以拿到信息
    if(allLoginFails.length >= maxFailTimes){
      //主输出流
      out.collect(LoginWarning(allLoginFails.head.userId, allLoginFails.head.timestamp, allLoginFails.last.timestamp,
        "login failed " + maxFailTimes + " in " + timeGapSeconds + " s!"))
    }
    //清空状态
    loginFailState.clear()
  }*/

}


