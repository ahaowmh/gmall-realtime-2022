package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;


/**
 * @author ahao
 * @date 2022/7/20 15:16
 */
public class DwdTrafficUserJumpDetail {
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

        //TODO 2.从kafka的的dwd_traffic_page_log读取数据
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(
        "dwd_traffic_page_log", "dwd_traffic_user_jump_detail", "hadoop102:9092"));

        //TODO 3.转换数据为JSONObject并过滤脏数据  提取时间戳设置watermark
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){
        };
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    JSONObject jsonObject = JSON.parseObject(value);
                    out.collect(jsonObject);
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        }).assignTimestampsAndWatermarks(WatermarkStrategy.<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                .withTimestampAssigner(new SerializableTimestampAssigner<JSONObject>() {
            @Override
            public long extractTimestamp(JSONObject element, long recordTimestamp) {
                return element.getLong("ts");
            }
        }));

        //将不符合规则的数据从侧输出流打印到控制台
        jsonObjectDS.getSideOutput(dirtyTag).print("脏数据>>>>>>");

        //TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.定义模式序列  两个规则都是last_page_id == null,严格有序
        Pattern<JSONObject, JSONObject> pattern = Pattern.<JSONObject>begin("start").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).next("next").where(new SimpleCondition<JSONObject>() {
            @Override
            public boolean filter(JSONObject value) throws Exception {
                return value.getJSONObject("page").getString("last_page_id") == null;
            }
        }).within(Time.seconds(10));

        //TODO 6.将模式序列作用到流上
        PatternStream<JSONObject> patternStream = CEP.pattern(keyedStream, pattern);

        //TODO 7.提取符合模式序列的事件和超时事件
        OutputTag<String> timeOutTag = new OutputTag<String>("TimeOut"){
        };

        //获取满足cep规则的数据流
        SingleOutputStreamOperator<String> selectDS = patternStream.select(
                timeOutTag,
                new PatternTimeoutFunction<JSONObject, String>() {
            @Override
            public String timeout(Map<String, List<JSONObject>> pattern, long timeoutTimestamp) throws Exception {
                return pattern.get("start").get(0).toJSONString();
            }
        }, new PatternSelectFunction<JSONObject, String>() {
            @Override
            public String select(Map<String, List<JSONObject>> pattern) throws Exception {
                return pattern.get("start").get(0).toJSONString();
            }
        });
        //获取超时事件流
        DataStream<String> timeOutDS = selectDS.getSideOutput(timeOutTag);

        //TODO 8.合并两个流
        DataStream<String> unionDS = selectDS.union(timeOutDS);

        //TODO 9.将数据写出到Kafka
        selectDS.print("selectDS>>>>>>");
        timeOutDS.print("timeOutDS>>>>>>");
        unionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_traffic_user_jump_detail"));

        //TODO 10.启动任务
        env.execute("DwdTrafficUserJumpDetail");
    }
}
