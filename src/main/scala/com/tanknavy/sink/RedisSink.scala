package com.tanknavy.sink

import com.tanknavy.source.bounded.SensorReading
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}

/**
 * Author: Alex Cheng 11/28/2020 9:15 PM
 */

object RedisSink {
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
      data =>{
        val dataArray = data.split(",")
        //SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //为了方便序列化写到kafka
        SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
      })

    //sink到redis, flink官方实现了kafka的sink
    val conf = new FlinkJedisPoolConfig.Builder().setHost("localhost").setPort(6379).build() //redis配置
    dataStream.addSink(new RedisSink[SensorReading](conf, new MyRedisMapper))

    dataStream.print("redis sink")

    env.execute("kafka sink test")
  }
}


//flink数据映射到redis
class MyRedisMapper extends RedisMapper[SensorReading]{ //泛型类型就是输入数据类型SendorReading

  //数据保存到redis
  override def getCommandDescription: RedisCommandDescription = {
    //数据保存为redis的哈希表hashtable类型,使用命令 HSET key field value
    new RedisCommandDescription(RedisCommand.HSET,"sensor_temp")
  }

  //保存到redis的key
  override def getKeyFromData(t: SensorReading): String = t.id

  //保存到redis的value
  override def getValueFromData(t: SensorReading): String = t.temperature.toString
}