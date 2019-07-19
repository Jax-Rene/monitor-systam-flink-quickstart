package me.zjy;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhuangjy
 * @create 2019-07-18 20:20
 */
public class Main {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 8080);
        text.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String value) throws Exception {
                String[] items = value.split(" ");
                String host = items[1];
                String cpu = items[2];
                double usage = Double.parseDouble(items[3]);
                return new Tuple3<>(host, cpu, usage);
            }
        }).filter(new FilterFunction<Tuple3<String, String, Double>>() {
            @Override
            public boolean filter(Tuple3<String, String, Double> value) throws Exception {
                // 返回超过90保留，否则过滤掉
                return value.f2 > 90;
            }
        }).print();
        env.execute("Window WordCount");
    }

}
