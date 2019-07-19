package me.zjy;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author zhuangjy
 * @create 2019-07-18 21:58
 */
public class ComputeCpuAvg {

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
        }).keyBy(0)
                // 设置时间窗口，每分钟滚定一次，读取最近一分钟的数据
                .timeWindow(Time.minutes(1))
                // 聚合函数，计算每个主机的数据条数、总cpu使用率
                .aggregate(new AggregateFunction<Tuple2<String, Double>, Tuple2<Integer, Double>, Double>() {
                    @Override
                    public Tuple2<Integer, Double> createAccumulator() {
                        // 初始化一个累加器
                        return new Tuple2<>(0, 0.0);
                    }

                    @Override
                    public Tuple2<Integer, Double> add(Tuple2<String, Double> value, Tuple2<Integer, Double> accumulator) {
                        // 新元素产生时加到现有累加器上
                        accumulator.f0 += 1;
                        accumulator.f1 += value.f1;
                        return accumulator;
                    }

                    @Override
                    public Double getResult(Tuple2<Integer, Double> accumulator) {
                        // 获取累加器对应值（计算均值）
                        return accumulator.f0 == 0 ? 0.0 : accumulator.f1 / accumulator.f0;
                    }

                    @Override
                    public Tuple2<Integer, Double> merge(Tuple2<Integer, Double> a, Tuple2<Integer, Double> b) {
                        // 合并累加器,可重用对象
                        a.f0 += b.f0;
                        a.f1 += b.f1;
                        return a;
                    }
                }).print();
        env.execute("ComputeCpuAvg");
    }

}
