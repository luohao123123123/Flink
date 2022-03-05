package flink

import org.apache.flink.streaming.api.functions.source.SourceFunction

class scala_MySource_03 extends SourceFunction[(String,Long)] {
  //是否终止数据流的标记
  var flag = true;

  override def run(sourceContext: SourceFunction.SourceContext[(String,Long)]):
  Unit = {
    while (flag) { //如果流没有终止，继续获取数据
      for(i<- 0 to 10000){
        sourceContext.collect(i.toString,System.currentTimeMillis())
        Thread.sleep(1000)
      }
     //每发送一次数据休眠1秒
      }
    }

  override def cancel(): Unit ={
    flag=false
  }

}

