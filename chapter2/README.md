
# 初识有状态计算

在上一章节中，我们构建了一个实时监控CPU使用率的报警程序。

但是该程序还有很多的缺陷，例如该程序虽然能计算实时数据，但是都是无状态的。

> 什么是无状态、有状态计算：
> 
> 无状态流处理分别接收每条记录，然后根据最新输入的记录输出记录。有状态的计算会维护状态（根据每条输入记录进行更新），并给予最新输入的记录和当前的状态值生成输出记录。

这次，我们希望在上一个程序的基础上实现两种监控：

1. 监控CPU峰值：实时监控所有主机CPU使用率，每次打印出该机器历史峰值
2. 监控CPU均值：每分钟监控最近一分钟每台机器CPU平均使用率，打印主机以及平均CPU使用率信息
3. 监控CPU中位数：每分钟监控最近一分钟每台机器CPU平均使用率，打印主机以及平均CPU使用率中位数

本次的这两种策略都是有状态的，因为每次处理完数据，都需要维护状态。

## 1. 编写程序 - 监控CPU峰值

这里，我们来实现监控CPU峰值程序。

``` java
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
env.execute("ComputeCpuMax")
```

我们在现有的案例中，仅仅做了非常小的改动就实现了，我们的需求。

我们使用keyBy函数实现了按主机聚合的操作，函数参数为作为agg key的元素索引，host在我们三元组中处于第一个元素，因此这里填写0.
``` java
keyBy(0)
```

然后，我们调用状态函数max函数，实时计算当前值和当前已记录的峰值的最大值，若当前值大于峰值则更新，并且打印记录。


## 2. 运行程序 - 监控CPU峰值

我们先后输入数据：

```
1563452056 10.8.22.1 cpu0 80.5
1563452050 10.8.22.1 cpu0 78.4
1563452056 10.8.22.1 cpu0 99.9
```

接下来会实时看到程序终端打印结果：

```
3> (10.8.22.1,cpu0,80.5)
3> (10.8.22.1,cpu0,80.5)
3> (10.8.22.1,cpu0,99.9)
```

从结果，我们可以看到我们状态被实时更新，并且最大值也在收到数据时实时打印。这里我们就完成了一个最简单的状态计算程序。

## 3. 编写程序 - 监控CPU均值

这里，我们来实现监控CPU均值程序，这个案例与上面不同的是上面的案例每条数据对会触发完成的计算输出结论，这里我们引入了Flink时间窗口API。

时间窗口是Flink的核心特性之一，时间窗口表示程序可以获取某段指定时间的数据针对这段时间的数据做一定的计算。因为在流操作中，数据是无穷无尽的，因此均值、中位值等计算通常是指一定时间窗口内的数据，如最近一分钟的数据。

> 在监控系统中，时间窗口也具有极高的价值，因为我们通常关注的计算策略通常为最近N分钟，指标的某种聚合结果。

Flink还提供了滑动窗口、技数窗口、会话窗口等高级特性，我们这里先只关注基本的滚动窗口使用，下一章节我们再针对窗口实现原理做深入学习。

``` java
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
                    public Tuple2<Integer, Double> add(Tuple2<String, Double> value, Tuple2<Integer, accumulator) {
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
```

这次的代码看起来比先前长了很多，我们先是对先前keyBy的结果使用了时间窗口：

``` java
.timeWindow(Time.minutes(1));
```

该代码表示对每个收到数据的主机每分钟会申明一个滚动时间窗口，计算最近这一分钟内的数据（这里频率以及窗口大小都是一分钟，因此是滚动窗口）。

接下来我们要实现求均值，我们知道求均值的计算方式是：

``` 
（CPU使用率总和） / 数据条数
```

因此，我们申明了一个增量累加函数 aggregate，该函数定义为：

``` java
public <ACC, R> SingleOutputStreamOperator<R> aggregate(AggregateFunction<T, ACC, R> function)
```

其中，T为我们的上游数据类型，ACC是累加数值的类型，R是返回的结果值。该函数的原理为窗口中一开始没数据时定义一个默认的ACC元素，然后有数据产生时调用add方法累加到ACC元素上，
实际上在这里merge函数不会被触发（添加日志后证实），merge函数只有在窗口合并的时候调用，合并两个容器，当窗口函数到达触发时刻调用getResult方法返回结果。

我们可以看到，整个过程是增量计算的（无需保留所有原始数据），因此在大数据量时是非常高效的。常用的增量计算函数还有 reduce 函数，我们在开发中应该优先使用增量计算函数减少资源的浪费。


## 4. 运行程序 - 监控CPU均值

输入数据：
```
1563452056 10.8.22.1 cpu0 80.5
1563452050 10.8.22.1 cpu0 78.4
1563452056 10.8.22.1 cpu0 99.9
1563452056 10.8.22.2 cpu1 20.2
```

等了一分钟后程序终端输出
```
3> 86.26666666666667
3> 20.2
```

说明主机都被正确计算了均值。

又等了若干分钟后，由于没输入数据，在终端也再看不到数据输出了，说明滚动窗口不断产生不断迭代。


## 5. 编写程序 - 监控CPU中位数

监控CPU中位数计算方式同均值类似，不过中为数由于需要保留所有的数据排序后得到因此不可进行增量计算。因此这里我们在timeWindow后使用了process方法。

process方法可以申明一个 ProcessWindowFunction 实现类，该实现类主要覆写了方法：

```java
	/**
	 * Evaluates the window and outputs none or several elements.
	 *
	 * @param key The key for which this window is evaluated.
	 * @param context The context in which the window is being evaluated.
	 * @param elements The elements in the window being evaluated.
	 * @param out A collector for emitting elements.
	 *
	 * @throws Exception The function may throw exceptions to fail the program and trigger recovery.
	 */
	public abstract void process(KEY key, Context context, Iterable<IN> elements, Collector<OUT> out) throws Exception;
```

从注释中，我们可以知道该函数的几个参数含义：

* key: 窗口对应的Key值，例如这里我们的Key值实际上就是ip地址（KeyBy内容）
* context：正在运行的窗口上下文，包括开始时间、结束时间等元信息
* elements: 窗口中的所有正在被计算的元素
* out: 数据结果收集器

调整后的代码如下：

``` java
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
        }).keyBy(0).timeWindow(Time.minutes(1)).process(new ProcessWindowFunction<Tuple2<String, Double>, Tuple, TimeWindow>() {
    @Override
            public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Double>> elements, CoDouble> out) throws Exception {
        List<Double> values = new ArrayList<>();
        elements.forEach(t -> values.add(t.f1));
        Collections.sort(v                
        if (values.isEmpty()) {
            out.collect(0.0);
        } else if (values.size() % 2 != 0) {
            out.collect(values.get(values.size() / 2));
        } else {
            out.collect((values.get(values.size() / 2) + values.get(values.size() / 2 - 1)) / 2);
        }
    }
}).print();
env.execute("ComputeCpuMiddle")
```

我们实现了自己的ProcessWindowFunction实现类，以及其对应的process方法。在该方法中，我们重排了窗口中的元素并且计算出中位值后返回。

> 从代码中我们可以看到process灵活度会比reduce、aggregate高很多。但是process由于需要计算所有数据，在数据量下，大窗口是非常影响程序运行效率的。因此能用reduce、aggregate的就千万不要使用process！


## 6. 运行程序 - 监控CPU中位数

输入数据：
```
1563452056 10.8.22.1 cpu0 80.5
1563452050 10.8.22.1 cpu0 78.4
1563452056 10.8.22.1 cpu0 99.9
1563452056 10.8.22.2 cpu1 20.2
```

输出数据：
```
3> 80.5
3> 20.2
```

中位数被正确计算出来。


## 总结

在这节中，我们通过计算：

1. CPU峰值
2. CPU均值
3. CPU中位值

学习到了有状态计算下一些核心知识点：

* 使用keyBy汇聚相同key的数据
* 滚动窗口api timeWindow的使用
* reduce/aggregate/process等窗口处理函数，并且明白了他们的区别（是否增量计算）

通过这些知识点，我们现在可以开发出很灵活的监控程序了。但是这还远远不够，因为我们目前的计算都是针对 *处理时间* 而非 *计算时间*，对于数据延迟、滑动窗口计算等问题均未考虑。

下一节，我们将学习watermark、数据时间等知识点，并且尝试更多的窗口类型，如计数窗口、会话窗口等，通过这些机制，我们可以定制出更准确、优雅的报警程序。