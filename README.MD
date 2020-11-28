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
.\bin\windows\kafka-topics --create --partitions 2 --replication-factor 1 --zookeeper localhost:2181 --topic salesorder
# console生产者
.\bin\windows\kafka-console-producer --broker-list  localhost:9092 --topic salesorder