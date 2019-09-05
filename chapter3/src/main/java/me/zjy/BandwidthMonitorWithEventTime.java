package me.zjy;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.concurrent.TimeUnit;

public class BandwidthMonitorWithEventTime {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        DataStream<String> text = env.socketTextStream("localhost", 8080);
        // 在执行任何操作前需要先执行watermark设置
        text.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.minutes(1L)) {
            @Override
            public long extractTimestamp(String element) {
                int time = (int) LocalDateTime.parse(element.split(" ")[0]).toEpochSecond(ZoneOffset.ofHours(8));
                return time * 1000L;
            }
        }).map(new MapFunction<String, Tuple3<Integer,String, Long>>() {
            @Override
            public Tuple3<Integer,String, Long> map(String s) throws Exception {
                String[] items = s.split(" ");
                // 返回 时间 -> channel -> 流量
                Integer time = (int) LocalDateTime.parse(items[0]).toEpochSecond(ZoneOffset.ofHours(8));
                String channel = items[1];
                Long flow = Long.parseLong(items[2]);
                return new Tuple3<>(time, channel, flow);
            }}).keyBy(1)
                .timeWindow(Time.minutes(5),Time.seconds(5))
                .reduce((ReduceFunction<Tuple3<Integer, String, Long>>) (integerStringLongTuple3, t1) -> new Tuple3<>(integerStringLongTuple3.f0,integerStringLongTuple3.f1,integerStringLongTuple3.f2 + t1.f2))
                .map(new MapFunction<Tuple3<Integer, String, Long>, Tuple2<String,Double>>() {
                    @Override
                    public Tuple2<String, Double> map(Tuple3<Integer, String, Long> integerStringLongTuple3) throws Exception {
                        return new Tuple2<>(integerStringLongTuple3.f1,integerStringLongTuple3.f2 * 8.0 / 60 / 1024 / 1024);
                    }
                })
                // 过滤出带宽值低于100Mbps域名
                .filter((FilterFunction<Tuple2<String, Double>>) stringDoubleTuple2 -> stringDoubleTuple2.f1 < 100.0).print();

        env.execute("BandwidthMonitorWithEventTime");
    }



}
