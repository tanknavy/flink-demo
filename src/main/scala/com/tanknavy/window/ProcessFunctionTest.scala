package com.tanknavy.window

import com.tanknavy.source.bounded.SensorReading
import org.apache.flink.api.common.functions.{AggregateFunction, FlatMapFunction, RichFlatMapFunction}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.util.Collector

/**
 * Author: Alex Cheng 11/30/2020 7:33 PM
 */

//ProcessFunction，可以访问时间戳，watermark, 注册定时时间，用来构建事件驱动的引用已经自定义业务逻辑(之前window函数和转换算子无法实现的)
//Flink SQL就是使用Process Function实现
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    //1.环境env
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //并行度
    env.setParallelism(1)

    //event time而不是默认的process time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime) //指定事件time，记得还要指定如何抽取event time
    env.getConfig.setAutoWatermarkInterval(500L) //默认200ms周期产生watermark

    //开启流状态的checkpoint容错机制，间隔时间，参数设置
    env.enableCheckpointing(60 * 1000) //间隔60s,
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(100 *1000) //超时时间
    env.getCheckpointConfig.setFailOnCheckpointingErrors(true) //检查点错误时要停掉job吗？
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1) //同时几个checkpoint，比如间隔太小导致检查点重叠
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION) //job被取消时还需要保存检查点信息吗
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 500)) //尝试几次

    //选择状态后端 @deprecated Use [[StreamExecutionEnvironment.setStateBackend(StateBackend)]] instead.
    //env.setStateBackend(new MemoryStateBackend()) //有三种，状态管理和checkpoint分别在本地task jvm和远程jobManger(都内存对象), 本地 task jvm和fs(内存和文件), 本地RocksDB中(DB)
    env.setStateBackend(new FsStateBackend("file:///e:/tmp/flink/checkpoint", false)) //状态在内存对象，检查点在fs


    //2.source文件中读取
    //实际常见是kafka source->flink->kafka sink
    //socket数据流
    val socketStream = env.socketTextStream("localhost", 7777) //linux:nc -lk 7777， window7：nc -lp 7777

    //文本文件数据源
    //val streamFromFile = env.readTextFile("D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\sensor.txt")

    //自定义类型数据源
    //val mySourceStream: DataStream[SensorReading] = env.addSource(new SensorSource()) //自定义数据源

    //2.1.基本转换算子和简单聚合算子, keyBy: DataStream -> KeyedStream, 然后可以agg/reduce
    //val dataStream = streamFromFile.map(
    val dataStream = socketStream.map(
      //val dataStream = mySourceStream.map(
      //val dataStream = kafkaStream.map(
      data => {
        val dataArray = data.split(",")
        //SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //为了方便序列化写到kafka
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })
      //水位线，三种格式的time assigner,使用event time时如果使用 time assingner产生watermark
      //.assignAscendingTimestamps(_.timestamp * 1000) //数据升序时，就不用watermark延迟触发，传入一个时间戳抽取器(毫秒)，到时间就触发不用延迟！
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) { //数据乱序，传入等待时间, wm = maxEventTs - waitTime
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
      })
    //.assignTimestampsAndWatermarks( new MyAssigner) //数据乱序，自定义time assigner

    //2.2开窗,时间窗口[)左边包括，右边不包含,使用window和聚合处理，单这些不能实现业务时，使用更底层的ProcessFunction,
    //2.2.1需求：15秒滑动窗口最小温度
    val minTempPerWindowStream = dataStream //DataStream[SensorReading]
      .map( data =>(data.id,data.temperature))
      .keyBy(_._1) //keyBy在water mark分配之后，
      //几种window用法示例
      //.timeWindow(Time.seconds(10))//10秒的滚动窗口,是window的简写
      //.timeWindow(Time.milliseconds(20))//毫秒单位，是window的简写
      //.timeWindow(Time.seconds(15),Time.seconds(5))//滑动窗口,15s内，每隔5s
      .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5),Time.hours(-8)))//滑动窗口, 多一个8hours的offset，时区
      .reduce( (d1,d2) => (d1._1, d1._2.min(d2._2)) ) //keyBy以后同分区的id一样, reduce做增量聚合，reduce后返回DataStream
      //.aggregate( new CountAgge(), new WindowResultFunction()) //除了reduce类似的增量聚合还有全窗口聚合,两个参数，一个聚合规则，一个输出数据结构
      //.apply()
      //.process() //大招

    //业务逻辑
    //2.2.2需求: 10s秒内温度连续两次上升，window开窗和reduce都不能实现时，使用更底层的ProcessFunction,它可以访问watermark
    val processedStream = dataStream
        .keyBy(_.id)
        .process( new TempIncreAlert() ) //keyedProcessFunction，如果温度比上次高，则创建一个1s后的timer,如果温度下降，则删除当前timer

    //2.2.3需求: 两次温差超过threshold就报警，process是大招
    val processedStream2 = dataStream
        .keyBy(_.id)
        .process(new TempChangeAlert(10)) //只是简单比较两次温差，有必要使用process吗

    //2.2.4需求同上，温差报警，但是不用process来，使用RichFlatMapFunction,也带有状态
    val processedStream3 = dataStream
        .keyBy(_.id)
        .flatMap(new TempChangeAlert2(10)) //不使用KeyedProcessFunction,而是RichFlatMapFunction,也带有状态

    //2.3.5需求同上，温差报警，但是不用process来，使用RichFlatMapFunction,也带有状态
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

    //3. sink,使用print
    //dataStream.addSink(new MyJdbcSink())
    dataStream.print("data stream")
    //minTempPerWindowStream.print("min temp") //没有输出，原因1,默认时间窗口按照process time而不是event time, 原因2，窗口时间太长，还没来得及关窗户计算程序就结束了
    processedStream.print("alert incre in 10s") //10秒内温度连续上升则报警，timer实现10秒后callback
    //温差太大报警，使用KeyedProcessFunction
    processedStream2.print("alert for big diff with process")
    //温差太大报警，使用RichFlatMapFunction
    processedStream3.print("alert for big diff with flatMap")
    //温差太大报警，使用FlatMapWithStatus, 直接使用函数式编程
    processedStream4.print("alert for big diff with flatMapWithState")


    //4. 执行
    env.execute("window test")

  }

}




//传感器10秒内温度连续上升就报警: 温度比上次高，设定10后定时器，如果温度减低，删除设定的定时器
//window和聚合函数无法实现，使用ProcessFunction，针对每个key，所以先要keyBy
//算子状态和键控状态分别有几种数据结构，比如值状态(本类),列表状态，映射状态，聚合状态，广播状态
class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String]{ //[key, in, out]

  //定义状态，保存上一个数据的温度值，如果不使用lazy可以在open方法生命周期中
  lazy val lastTemp : ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))//上次数据的温度作为状态
  //定义状态，保存上一个数据的event time
  lazy val lastTs : ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastTs", classOf[Long]))//上次数据的event time作为状态
  //定义状态，保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer",classOf[Long]))

  //#Context是KeyedProcessFunction的内部类
  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //先取出上一个温度值
    val preTemp = lastTemp.value()
    //上一个数据的时间戳
    val preTs = lastTs.value()
    //更新lastTemp状态为现在数据的
    lastTemp.update(value.temperature)
    lastTs.update(value.timestamp)

    val curTimerTs = currentTimer.value() //currentTimer默认值为0.0


    //温度上升,并且还没有定时器时注册定时器, 如果温度下降了, 则删除定时器
    //if(value.temperature > preTemp){ //如果数据乱序的还能直接比较么,不结合event time吗？
    //if(value.temperature > preTemp && currentTimer == 0){ //如果数据乱序的还能直接比较么,不结合event time吗？
    if(value.timestamp > preTs && value.temperature > preTemp && curTimerTs == 0){ //如果数据乱序的还能直接比较么,不结合event time吗？
      val timerTs = ctx.timerService().currentProcessingTime() + 10000L //当前处理时间后10秒触发定时器闹钟
      ctx.timerService().registerProcessingTimeTimer( timerTs)//注册定时器(使用处理时间),这里要求查看10s内温度连续上升，那就1秒后执行，时间戳 = 当前处理时间 + 1s
      currentTimer.update(timerTs) //保存定时器状态,用于删除
     //温度下降，或者第一条数据，删除定时器并清空状态
    //}else if(value.temperature < preTemp || preTemp == 0.0) { //后面preTemp == 0.0是刚启动时上次温度初始化为0.0,这时温度和0.0比较没必要设定timer
    }else if( (value.timestamp >= preTs && value.temperature <= preTemp) || preTemp == 0.0) { //后面preTemp == 0.0是刚启动时上次温度初始化为0.0,这时温度和0.0比较没必要设定timer
      ctx.timerService().deleteProcessingTimeTimer(curTimerTs) //删除timer
      currentTimer.clear() //删除定时器后清空定时时间戳这个状态，为下次定时时间戳做准备
    }

  }

  //回调，时间到了触发什么操作，这里out输出报警信息
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //super.onTimer(timestamp, ctx, out)
    out.collect(ctx.getCurrentKey + "温度连续上升告警!") //当前分区键 + alert
    currentTimer.clear()//报警后，清空定时时间戳状态，为下次报警时间戳做准备
  }
}


//两次温度差值过大就报警: 温度比上次高，
//window和聚合函数无法实现，使用ProcessFunction，
//算子状态和键控状态分别有几种数据结构，比如值状态(本类),列表状态，映射状态，聚合状态，广播状态
class TempChangeAlert(threshold: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] { //[key, in, out]

  //定义一个状态，保存上次的温度值,用于process方法中和当前数据比较,
  lazy val lastTempState : ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, out: Collector[(String, Double, Double)]): Unit = {
    //状态中保存上次温度
    val lastTemp = lastTempState.value()
    val diff = (value.temperature - lastTemp).abs //scala函数式编程
    if(diff > threshold){ //初次误报
      out.collect((value.id, lastTemp, value.temperature)) //keyBy后的,
    }
    lastTempState.update(value.temperature) //记得更新最近温度状态
  }
}

//功能同上，但不使用process，而是flatMap中使用，这里需要状态, Rich函数有状态的(getRuntimeContext.getState)
class TempChangeAlert2(threshold:Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)]{

  //定义一个状态，保存上次的温度值,用于process方法中和当前数据比较,
  lazy val lastTempState : ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  //也可使使用生命周期方法open初始化lastTempState
  private var lastTempState2: ValueState[Double] = _ //在open中初始化


  override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = { //in, out
    //状态中保存上次温度
    val lastTemp = lastTempState.value()
    val diff = (value.temperature - lastTemp).abs //scala函数式编程
    if(diff > threshold){ //初次误报
      out.collect((value.id, lastTemp, value.temperature)) //keyBy后的,
    }
    lastTempState.update(value.temperature) //记得更新最近温度状态
  }

  override def open(parameters: Configuration): Unit = {
    //和上面lazy 延迟赋值效果完全一样
    lastTempState2 = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp2", classOf[Double]))
  }
}


//aggregate全窗口聚合用,累加器
class CountAgge2() extends AggregateFunction[SensorReading, Long, Long]{ //in, acc, out
  override def createAccumulator(): Long = ???

  override def add(in: SensorReading, acc: Long): Long = ???

  override def getResult(acc: Long): Long = ???

  override def merge(acc: Long, acc1: Long): Long = ???
}


//aggregate全窗口聚合用, apply定了要输出的数据类型
class WindowResultFunction2() extends WindowFunction[Long, Long, String, TimeWindow]{ //in, out, key,W
  override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[Long]): Unit = {

  }
}