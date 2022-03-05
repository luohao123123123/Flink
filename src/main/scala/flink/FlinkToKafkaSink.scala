package flink

import Util.KafkaUtil
import org.apache.flink.api.common.serialization.SimpleStringSchema

import java.lang
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaProducer, KafkaSerializationSchema}
import org.apache.kafka.clients.producer.ProducerRecord

//TODO:Flink数据写入Kafka
object FlinkToKafkaSink {
  def main(args: Array[String]): Unit = {
//    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    streamEnv.setParallelism(1) //默认情况下每个任务的并行度为1
//    import org.apache.flink.streaming.api.scala._
//
//    //读取netcat流中数据 （实时流）
//    val stream1: DataStream[String] = streamEnv.socketTextStream("192.168.213.60", 2222)
//
//    //转换计算
//    val result = stream1.flatMap(_.split(","))
//      .map((_, 1))
//      .keyBy(0)
//      .sum(1)
//
//    //Kafka生产者的配置
//    val props = KafkaUtil.getProducer
//
//    //数据写入Kafka，并且是KeyValue格式的数据
//    result.addSink(new FlinkKafkaProducer[(String, Int)]()("topic1",
//      new KafkaSerializationSchema[(String, Int)] {
//        override def serialize(element: (String, Int), aLong: lang.Long): ProducerRecord[Array[Byte], Array[Byte]] = {
//          new ProducerRecord("topic1", element._1.getBytes, (element._2 + "").getBytes())
//        }
//      }, props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE)) //EXACTLY_ONCE 精确一次
//    streamEnv.execute()

    //todo:方法二：
    val streamEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    streamEnv.setParallelism(1) //默认情况下每个任务的并行度为1
    import org.apache.flink.streaming.api.scala._

    //读取netcat流中数据 （实时流）
    val data: DataStream[String] = streamEnv.socketTextStream("192.168.213.60", 2222)

    //转换计算
    val result = data.flatMap(_.split(","))

    //数据写入Kafka，并且是KeyValue格式的数据
    val kafkabrokerList="master:9092,slave1:9092,slave2:9092"
    result.addSink(new FlinkKafkaProducer[String](kafkabrokerList, "topic01", new SimpleStringSchema()))
    data.print()
    streamEnv.execute()
  }
}
