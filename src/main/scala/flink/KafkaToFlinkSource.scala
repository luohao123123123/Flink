package flink

import Util.KafkaUtil
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer, KafkaDeserializationSchema}
import org.apache.kafka.clients.consumer.ConsumerRecord

//TODO:Flink读取kafka数据
object KafkaToFlinkSource {
  def main(args: Array[String]): Unit = {
    //初始化Flink的Streaming（流计算）上下文执行环境
    val streamEnv = StreamExecutionEnvironment.getExecutionEnvironment
    //设置并行度
    streamEnv.setParallelism(1)
    //导入隐式转换，建议写在这里，可以防止IDEA代码提示出错的问题
    import org.apache.flink.streaming.api.scala._

    //创建kfka连接
    val props = KafkaUtil.getConsumerProperties

    val stream = streamEnv.addSource(new
        FlinkKafkaConsumer[(String, String)]("topic01", new KafkaDeserializationSchema[(String, String)] {

          //流是否结束
          override def isEndOfStream(t: (String, String)) = false

          override def deserialize(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]]) = {
            if (consumerRecord != null) {
              var key = "null"
              var value = "null"
              if (consumerRecord.key() != null)
                key = new String(consumerRecord.key(), "UTF-8")
              if (consumerRecord.value() != null)
                value = new String(consumerRecord.value(), "UTF-8")
              (key, value)
            } else { //如果kafka中的数据为空返回一个固定的二元组
              ("null", "null")
            }
          }
          //设置返回类型为二元组
          override def getProducedType: TypeInformation[(String, String)] =
            createTuple2TypeInformation(createTypeInformation[String], createTypeInformation[
              String])
        }
          , props).setStartFromEarliest())

    stream
      .map(_._2)
      .print()

    streamEnv.execute()
  }
}

