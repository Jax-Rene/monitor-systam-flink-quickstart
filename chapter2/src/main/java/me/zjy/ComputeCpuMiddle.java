package me.zjy;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author zhuangjy
 * @create 2019-07-18 21:58
 */
public class ComputeCpuMiddle {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> text = env.socketTextStream("localhost", 8080);
        text.map(new MapFunction<String, Tuple2<String, Double>>() {
            @Override
            public Tuple2<String, Double> map(String value) throws Exception {
                String[] items = value.split(" ");
                String host = items[1];
                double usage = Double.parseDouble(items[3]);
                return new Tuple2<>(host, usage);
            }
        }).keyBy(0).timeWindow(Time.minutes(1)).process(new ProcessWindowFunction<Tuple2<String, Double>, Double, Tuple, TimeWindow>() {
            @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Double>> elements, Collector<Double> out) throws Exception {
                List<Double> values = new ArrayList<>();
                elements.forEach(t -> values.add(t.f1));
                Collections.sort(values);

                if (values.isEmpty()) {
                    out.collect(0.0);
                } else if (values.size() % 2 != 0) {
                    out.collect(values.get(values.size() / 2));
                } else {
                    out.collect((values.get(values.size() / 2) + values.get(values.size() / 2 - 1)) / 2);
                }
            }
        }).print();
        env.execute("ComputeCpuMiddle");
    }

}
