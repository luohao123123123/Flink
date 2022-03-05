package Util;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import scala.collection.Map;

import java.util.HashMap;
import java.util.Properties;


public class KafkaUtil {
    //获取包含Kafka连接参数的ProducerProperties
    public static Properties getProducerProperties(){
        Properties props = new Properties();
        // 连接Kafka集群broker-list
        props.put("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");
        // 应答机制，0,1，all=-1
        props.put("sacks", "all");
        // 重试次数
        props.put("retries", 0);
        //RecordAccumulator 缓冲区大小
        props.put("buffer.memory", 33554432);
        // key序列化
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    //获取包含Kafka连接参数的ConsumerProperties
    public static Properties getConsumerProperties(){
        Properties props = new Properties();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        props.put("bootstrap.servers", "master:9092,slave1:9092,slave2:9092");
        // 制定consumer group
        props.put("group.id", "test");
        // 是否自动确认offset
        props.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        props.put("auto.commit.interval.ms", "1000");
        // key的序列化类
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //读取之前的所有数据
//        props.put("auto.offset.reset", "earliest");
//        props.put("group.id", UUID.randomUUID().toString());
        // 定义consumer
        return props;
    }

    //TODO:消费者
    //获取KafkaConsumer<String, String>，可自定义包含Kafka参数的Properties
    public static KafkaConsumer<String, String> getConsumer(Properties props){
        return new KafkaConsumer<> (props);
    }

    //获取KafkaConsumer<String, String>
    public static KafkaConsumer<String, String> getConsumer(){
        Properties props = getConsumerProperties();
        return new KafkaConsumer<> (props);
    }

    //TODO:生产者

    //获取KafkaProducer<String, String>,可自定义包含Kafka参数的Properties
    public static KafkaProducer<String, String> getProducer(Properties props){
        return new KafkaProducer<>(props);
    }
    public static KafkaProducer<String, String> getProducer(){
        Properties props = getProducerProperties();
        return new KafkaProducer<>(props);
    }
    
    //TODO:Streaming-Kafka
    public static Map<String,Object> getStreaming_KafkaConsumer(){
        java.util.Map<String,Object> map=new HashMap();
        // 定义kakfa 服务的地址，不需要将所有broker指定上
        map.put("bootstrap.servers","master:9092,slave1:9092,slave2:9092");
        // key的序列化类
        map.put("key.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // value的序列化类
        map.put("value.deserializer","org.apache.kafka.common.serialization.StringDeserializer");
        // 制定consumer group
        map.put("group.id", "test");
        // 是否自动确认offset
        map.put("enable.auto.commit", "true");
        // 自动确认offset的时间间隔
        map.put("auto.commit.interval.ms", "1000");

        //转scala.collection.Map
        return scala.collection.JavaConverters.mapAsScalaMapConverter(map).asScala();
    }
}
