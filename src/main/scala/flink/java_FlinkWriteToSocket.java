package flink;


import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



//TODO:自定义数据源发送写入到文本流
public class java_FlinkWriteToSocket {
    public static void main(String[] args) throws Exception {
        //local模式默认的并行度是当前节点逻辑核的数量
        StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStream的并行度
        streamEnv.setParallelism(1);


        DataStreamSource<String> lines= streamEnv.addSource(new java_MySource());
        lines.print();
        //这种方式就是把数据把自己输入的数据发送到端口号【类似java键盘扫描仪操作（打印操作）】
        lines.writeToSocket("192.168.213.60",2222,new SimpleStringSchema());


        streamEnv.execute();
    }
}
