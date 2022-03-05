package flink

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class scala_MySource_02 extends SourceFunction[String] {
  //是否终止数据流的标记
  var flag = true;

  override def run(sourceContext: SourceFunction.SourceContext[String]):
  Unit = {
    while (flag) { //如果流没有终止，继续获取数据
      for(i<- 0 to 10){
        sourceContext.collect(i+"")
      }
      Thread.sleep(3000) //每发送一次数据休眠2秒
      }
    }

  override def cancel(): Unit ={
    flag=false
  }

}

