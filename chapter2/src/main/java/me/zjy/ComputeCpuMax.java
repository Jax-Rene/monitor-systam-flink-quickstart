package me.zjy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhuangjy
 * @create 2019-07-18 21:13
 */
public class ComputeCpuMax {

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
        }).keyBy(0).max(2).print();
        env.execute("ComputeCpuMax");
    }

}
