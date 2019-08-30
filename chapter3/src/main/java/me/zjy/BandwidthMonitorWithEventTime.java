package me.zjy;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class BandwidthMonitorWithEventTime {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> text = env.socketTextStream("localhost", 8080);
        text.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String s) throws Exception {
                String[] items = s.split(" ");
                // 返回 channel -> 流量
                return new Tuple2<>(items[1], Long.parseLong(items[2]));
            }
        }).keyBy(0)
                // 滚动窗口
                .timeWindow(Time.minutes(1))
                // 时间窗口
//                .timeWindow(Time.minutes(1), Time.seconds(15))
                .reduce((ReduceFunction<Tuple2<String, Long>>) (stringLongTuple2, t1) -> new Tuple2<>(stringLongTuple2.f0, stringLongTuple2.f1 + t1.f1))
                // 过滤出带宽值低于100Mbps域名
                .filter((FilterFunction<Tuple2<String, Long>>) stringLongTuple2 -> stringLongTuple2.f1 * 8.0 / 60 / 1024 / 1024 < 100)
                .print();

        env.execute("BandwidthMonitor");
    }

}
