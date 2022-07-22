package com.atguigu.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.app.func.DimSinkFunction;
import com.atguigu.app.func.TableProcessFunction;
import com.atguigu.bean.TableProcess;
import com.atguigu.utils.MyKafkaUtil;
import com.sun.xml.internal.bind.v2.TODO;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @author ahao
 * @date 2022/7/18 21:28
 */
public class DimApp {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        /*// 状态后端设置 (生产环境要写）
        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        env.setRestartStrategy(RestartStrategies.failureRateRestart(
                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
        ));
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        System.setProperty("HADOOP_USER_NAME", "ahao");*/

        //TODO 2.从kafka读取数据创建流（topic_db)
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(
                "topic_db", "flink_realtime_dim","hadoop102:9092"));

        //TODO 3.将数据转换为josn并过滤掉脏数据   主流
        //用flatmap也可以实现转换数据格式并过滤数据，但是无法将脏数据保存到侧输出流
        OutputTag<String> dirty = new OutputTag<String>("Dirty"){

        };
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                //如果数据为null或者非JSON格式，作为异常捕获,写入侧输出流
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirty, value);
                }
            }
        });
        //可以在此将脏数据打印出来
        jsonObjectDS.getSideOutput(dirty).print("脏数据>>>>");

        //TODO 4.使用flinkCDC从读取配置信息创建配置流
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("09061954")
                .databaseList("gmall_config")
                .tableList("gmall_config.table_process")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        DataStreamSource<String> mysqlDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MysqlSource");


        //TODO 5.将配置流处理成广播流
        //广播流可以使每个并行度都会有完整的配置信息（需要的维度表名）
        MapStateDescriptor<String, TableProcess> stateDescriptor = new MapStateDescriptor<>(
                "map-state",
                String.class,
                TableProcess.class);
        BroadcastStream<String> broadcastDS = mysqlDS.broadcast(stateDescriptor);

        //TODO 6.连接主流和广播流
        BroadcastConnectedStream<JSONObject, String> broadcastConnectedStream = jsonObjectDS.connect(broadcastDS);

        //TODO 7.根据广播流处理主流  （方法逻辑复杂，将其单独写在func包中）
        SingleOutputStreamOperator<JSONObject> hbaseDS = broadcastConnectedStream.process(new TableProcessFunction(stateDescriptor));

        hbaseDS.print(">>>>>>>>>>");

        //TODO 8.将处理后的数据写入到phoenix
        hbaseDS.addSink(new DimSinkFunction());


        //TODO 9.执行任务
        env.execute("DimApp");
    }
}
