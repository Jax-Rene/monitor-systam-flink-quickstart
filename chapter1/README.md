
# 第一个实时报警程序

我们希望构建一个实时监控系统，假设我们有非常多的机器。每台机器都会定时上一个CPU使用率指标。上报格式为CSV，如下所示：

``` 
1563452056 10.8.22.1 cpu0 80.5
1563452055 10.8.22.1 cpu1 77.5
1563452051 10.8.22.1 cpu2 10.5
1563452058 10.8.22.1 cpu2 100.0
1563452058 10.8.22.2 cpu2 10.0
...
```

上面的数据分别表示：
1. 机器上报指标的时间
2. 机器IP
3. 哪个cpu核上报的
4. cpu使用率

每台机器每分钟会上报自己的每个核的使用率（即每分钟每个核最多一条数据）。

我们知道，cpu使用率太高的话机器可能就无法正常运作了。因此有效、快速的监控到机器cpu跑高十分重要。

为了低时延监控，我们选择了Flink实现第一个实时报警程序，假设现在所有的 Cpu 数据都上报到了我们的一个 Socket Server， 我们的Flink程序会从Socket Server 实时拉取数据并进行监控。

## 监控策略

第一步，我们希望实时打印出所有的主机对应核以及cpu使用率，这样我们可以实时的看到所有上报的数据。

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
}).print();
env.execute("Window WordCount");
```

``` java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
```

这一行表示创建执行环境，是Flink的第一步。通过StreamExecutionEnvironment这个入口类，我们可以用来设置参数和创建数据源以及提交任务。

下一步，我们创建了一个DataStream，该DataStream实时消费本地的8080 Socket Server数据。

再下一步，我们对DataStream做了一系列操作，通过Map算子将数据转化为 (host,cpu核,value) 的三元组，然后调用了print实时打印出上面的三元组。

``` java
env.execute("Window WordCount");
```  

最后的 env.execute 调用是启动实际Flink作业所必需的。所有算子操作（例如创建源、聚合、打印）只是构建了内部算子操作的图形。只有在execute()被调用时才会在提交到集群上或本地计算机上执行。

## 运行程序

我们使用nc命令模拟一个Socket Server：
``` bash
nc -lk 8080
```

启动程序后，flink自动连接上我们socket server并进行实时监听，我们在终端输入监控数据:

```
1563452056 10.8.22.1 cpu0 80.5
1563452051 10.8.22.1 cpu2 10.5
1563452051 10.8.22.1 cpu2 10.5
```

接下来会实时看到程序终端打印结果：

```
3> (10.8.22.1,cpu0,80.5)
4> (10.8.22.1,cpu2,10.5)
1> (10.8.22.1,cpu2,10.5)
```

说数据被实时收到并且处理了。

到这里，我们最简单的MVP版本已开发完毕，但是在使用中，我们会不断发现打出来的数据越来越多了，甚至是很多无用的数据。
例如，我们只关心cpu使用率 > 90 的数据，那么这时候我们可以使用Flink的filter算子帮我们过滤数据。
修改后的代码如下所示：

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
}).filter(new FilterFunction<Tuple3<String, String, Double>>() {
    @Override
    public boolean filter(Tuple3<String, String, Double> value) throws Exception {
        // 返回90保留否则过滤掉
        return value.f2 > 90;
    }
}).print();
env.execute("Window WordCount");
```

修改后，再输入：
```
1563452051 10.8.22.1 cpu2 10.5
1563452051 10.8.22.1 cpu2 99.2
```

发现仅仅输出后面那条数据：
```
2> (10.8.22.1,cpu2,99.2)
```

即我们有效的过滤了无用的数据，可以安心的看着我们的监控大屏啦！

## 总结

通过上面的案例，我们掌握了以下几个知识点：

* Flink程序开发的框架
* Map算子操作做数据转换、Filter算子操作过滤不需要的数据
* Flink事件驱动计算模式的开发