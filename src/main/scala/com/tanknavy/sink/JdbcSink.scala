package com.tanknavy.sink

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.tanknavy.source.bounded.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}

/**
 * Author: Alex Cheng 11/28/2020 11:45 PM
 */

object JdbcSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1) //在本地开发环境中，默认并行度是core数量个
    //实际常见是kafka source->flink->kafka sink
    //文本文件数据源
    val streamFromFile = env.readTextFile("D:\\Code\\Java\\IDEA\\FlinkTutorial\\src\\main\\resources\\sensor.txt")
    //自定义类型数据源
    //val mySourceStream: DataStream[SensorReading] = env.addSource(new SensorSource()) //自定义数据源

    //1.基本转换算子和简单聚合算子, keyBy: DataStream -> KeyedStream, 然后可以agg/reduce
    val dataStream = streamFromFile.map(
      //val dataStream = mySourceStream.map(
      //val dataStream = kafkaStream.map(
      data => {
        val dataArray = data.split(",")
        //SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //为了方便序列化写到kafka
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    //sink到mysql, flink官方实现了kafka的sink

    dataStream.addSink(new MyJdbcSink())

    dataStream.print("mysql sink")

    env.execute("mysql sink test")
  }


}

//Rich函数类有open,close的方法，用于flink写数据到jdbc时，先打开连接
class MyJdbcSink() extends RichSinkFunction[SensorReading]{
  //ctrl + O 要override的方法
  //定义sql连接, prepare statement
  var conn:Connection = _ //连接，使用_赋默认值为初始值，不然没有初始化下面方法中赋值会出错
  var insertStmt: PreparedStatement = _
  var updateStmt: PreparedStatement = _

  //初始化jdbc连接
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc://localhost:3306/test") //?????????????
    insertStmt = conn.prepareStatement("INSERT INTO flink-sensor(sensor,temp) VALUES(?,?)") //预编译语句，?占位符
    updateStmt = conn.prepareStatement("Update flink-sensor SET temp=? WHERE sensor=?")
  }

  //调用连接执行sql命令
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //执行更新语句，先查询
    updateStmt.setDouble(1, value.temperature)
    updateStmt.setString(2, value.id)
    updateStmt.execute()

    //如果update没有查到数据，那么执行插入数据
    if (updateStmt.getUpdateCount == 0){
      insertStmt.setString(1,value.id)
      insertStmt.setDouble(2, value.temperature)
      insertStmt.execute()
    }

  }

  //关闭时清理
  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    conn.close()
  }
}
