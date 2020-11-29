package com.tanknavy.sink

import java.util

import com.tanknavy.source.bounded.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
//import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.client.Requests

/**
 * Author: Alex Cheng 11/28/2020 9:40 PM
 * https://ci.apache.org/projects/flink/flink-docs-stable/dev/connectors/elasticsearch.html
 */

object EsSink {
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


    //sink到elasticSearch, flink官方实现了es的sink
    //es连接数
    val httpHosts = new util.ArrayList[HttpHost] //java.util下的arrayList
    //httpHosts.add(new HttpHost("192.168.1.187", 9200, "http")) //
    httpHosts.add(new HttpHost("spark3", 9200, "http")) //在localhost上写入失败，linux上成功

    //es builder
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHosts,
      new ElasticsearchSinkFunction[SensorReading] { //匿名类实现interface
        //flink记录如何写入es
        override def process(element: SensorReading, ctx: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data: " + element)

          //准备数据，包装成map或者json格式
          val json = new util.HashMap[String, String]
          json.put("sensor_id", element.id)
          json.put("temperature", element.temperature.toString)
          json.put("ts", element.timestamp.toString)

          //创建index request，准备post数据
          val indexRequest: IndexRequest = Requests.indexRequest
            .index("sensor")
            //.`type`("readingdata") //es6之前需要type,`type`因为type是scala中关键字
            .source(json)

          //利用index发送请求，写入数据
          requestIndexer.add(indexRequest)
          println("data saving in es index")

        }
      }
    )

    //配置bulk request, 默认会使用buffer
    esSinkBuilder.setBulkFlushMaxActions(1) //每个都写入
    //REST Client定制配置
//    esSinkBuilder.setRestClientFactory(new RestClientFactory {
//      override def configureRestClientBuilder(restClientBuilder: RestClientBuilder): Unit = {
//        //restClientBuilder.setDefaultHeaders()
//        restClientBuilder.setMaxRetryTimeoutMillis(1000)
//        //restClientBuilder.setPathPrefix("sen")
//        //restClientBuilder.setHttpClientConfigCallback()
//      }
//    })

    //flink data stream写入es sink
    dataStream.addSink( esSinkBuilder.build() ) //elasticSearch sink

    //打印输出看看
    //dataStream.print("es sink")

    //启动运行flink
    env.execute("es sink test")
  }
}
