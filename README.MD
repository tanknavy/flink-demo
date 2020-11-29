Flink DataSet and DataStream
Flink for e-commerce

flink start:
cd c:\software\flink-1.7.2
start-cluster.bat

kafka start:
#Zookeeper使用全局的或者自带的
cd C:\software\kafka_2.11-2.3.0
> zkserver
> .\bin\windows\zookeeper-server-start.bat config\zookeeper.properties
#启动kafka
.\bin\windows\kafka-server-start.bat .\config\server.properties
Localhost:9092

显示全部topic列表
.\bin\windows\kafka-topics --list --zookeeper localhost:2181
.\bin\windows\kafka-topics --describe --zookeeper localhost:2181 –topic 

# 创建topic
.\bin\windows\kafka-topics --create --partitions 2 --replication-factor 1 --zookeeper localhost:2181 --topic orderLog
# console生产者
.\bin\windows\kafka-console-producer --broker-list  localhost:9092 --topic orderLog
# console消费者
.\bin\windows\kafka-console-consumer --bootstrap-server localhost:9092 --topic orderLog
.\bin\windows\kafka-console-consumer --bootstrap-server localhost:9092 --topic orderLog --group my-first-application


redis start:
cd D:\Software\Redis
redis-server.exe redis.windows.conf
redis-cli.exe -h 127.0.0.1 -p 6379
>keys *
>hgetall sensor_temp


ElasticSearch
//直接es api查看全部index, 或者使用kabana
http://localhost:9200/_cat/indices?v
//sensor endpoint下查找
http://localhost:9200/sensor/_search?pretty