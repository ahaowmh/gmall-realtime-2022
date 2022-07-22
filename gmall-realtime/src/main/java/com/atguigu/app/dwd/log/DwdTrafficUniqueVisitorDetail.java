package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * 流量域独立访客事务事实表
 * 	过滤页面数据中的独立访客访问记录
 * @author ahao
 * @date 2022/7/20 09:50
 */
public class DwdTrafficUniqueVisitorDetail {
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

        //TODO 2.从kafka的dwd_traffic_page_log读取数据
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(
                "dwd_traffic_page_log", "unique_visitor_detail", "hadoop102:9092"));

        //TODO 3.转换为JSONObject并过滤last_page_id不为空的数据
        //将last_page_id为空或者非JSON格式的脏数据输出到侧输出流
        OutputTag<String> dirtyTag = new OutputTag<String>("Dirty"){
        };
        SingleOutputStreamOperator<JSONObject> jsonObjectDS = kafkaDS.process(new ProcessFunction<String, JSONObject>() {
            @Override
            public void processElement(String value, ProcessFunction<String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
                try {
                    //将数据转换为JSONObject
                    JSONObject jsonObject = JSON.parseObject(value);
                    //获取last_page_id，判断是否为空
                    if (jsonObject.getJSONObject("page").get("last_page_id") == null) {
                        out.collect(jsonObject);
                    }else {
                        //last_page_id为空
                        ctx.output(dirtyTag, value);
                    }
                    //非JSON格式的脏数据输出到侧输出流
                } catch (Exception e) {
                    ctx.output(dirtyTag, value);
                }
            }
        });
        jsonObjectDS.getSideOutput(dirtyTag).print("脏数据>>>>>>");


        //TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(json -> json.getJSONObject("common").getString("mid"));

        //TODO 5.利用状态编程过滤独立访客记录
        SingleOutputStreamOperator<JSONObject> filterDS = keyedStream.filter(new RichFilterFunction<JSONObject>() {
            //声明一个状态,维护一个时间
            private ValueState<String> lastDtState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //获取一个状态描述器
                ValueStateDescriptor<String> stateDescriptor = new ValueStateDescriptor<>("last_dt", String.class);
                //在状态中设置一个到期重置时间
                StateTtlConfig stateTtlConfig = new StateTtlConfig
                        .Builder(Time.days(1))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite).build();
                stateDescriptor.enableTimeToLive(stateTtlConfig);
                lastDtState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public boolean filter(JSONObject value) throws Exception {
                //获取当前数据中的ts和状态中的时间
                String lastDt = lastDtState.value();
                String curDt = DateFormatUtil.toDate(value.getLong("ts"));
                //如果状态为null或者状态日期与当前日期不同,则保留数据,更新状态
                if (lastDt == null || !lastDt.equals(curDt)) {
                    lastDtState.update(curDt);
                    return true;
                } else {
                    return false;
                }

            }
        });
        //将处理好的数据打印在控制台，用于测试
        filterDS.print(">>>>>>>");

        //TODO 6.将独立访客数据写入kafka
        //处理完的数据是JSONObject格式，封装好的KafkaProducer是String类型，要用map转换一下
        filterDS.map(josn -> josn.toJSONString())
                .addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_traffic_unique_visitor_detail"));


        //TODO 7.启动任务
        env.execute("DwdTrafficUniqueVisitorDetail");
    }
}
