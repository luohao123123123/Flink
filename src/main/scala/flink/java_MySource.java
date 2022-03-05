package flink;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class java_MySource implements SourceFunction<String> {
    Boolean flag=true;
    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        while (flag){
            for (int i = 0; i < 10; i++) {
                sourceContext.collect(i+"");
            }
            Thread.sleep(3000);
        }
    }

    @Override
    public void cancel() {
        flag=false;
    }
}
