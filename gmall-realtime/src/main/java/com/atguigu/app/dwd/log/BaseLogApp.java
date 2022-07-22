package com.atguigu.app.dwd.log;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author ahao
 * @date 2022/7/19 22:53
 */
public class BaseLogApp {
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

        //TODO 2.从kafka读数据创建流(topic_db)
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(
                "topic_log", "base_log_app", "hadoop102:9092"));

        //TODO 3.数据清洗并转换为JSON对象
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
        });
        jsonObjectDS.getSideOutput(dirtyTag).print("脏数据>>>>>>");

        //TODO 4.按照mid分组
        KeyedStream<JSONObject, String> keyedStream = jsonObjectDS.keyBy(json -> json.getJSONObject("common").getString("mid"));


        //TODO 5.使用状态编程实现新老用户校验
        SingleOutputStreamOperator<JSONObject> jsonObjWithNewFlagDS = keyedStream.map(new RichMapFunction<JSONObject, JSONObject>() {
            //定义一个状态
            private ValueState<String> valueState;

            //状态初始化
            @Override
            public void open(Configuration parameters) throws Exception {
                valueState = getRuntimeContext().getState(new ValueStateDescriptor<String>("first_dt", String.class));
            }

            @Override
            public JSONObject map(JSONObject value) throws Exception {
                //获取数据中的标记以及时间戳、获取状态数据
                String isNew = value.getJSONObject("common").getString("is_new");
                Long ts = value.getLong("ts");

                String curDt = DateFormatUtil.toDate(ts);
                String firstDt = valueState.value();

                if ("1".equals(isNew)) {

                    if (firstDt == null) {
                        //更新状态
                        valueState.update(curDt);
                    } else if (!firstDt.equals(curDt)) {
                        //更新标记为"0"
                        value.getJSONObject("common").put("is_new", "0");
                    }

                } else if (firstDt == null) {
                    //更新状态为昨日
                    String yesterday = DateFormatUtil.toDate(ts - 24 * 60 * 60 * 1000L);
                    valueState.update(yesterday);
                }

                //返回结果数据
                return value;
            }
        });

        //TODO 6.使用侧输出流进行日志分流
        //四条侧输出流
        OutputTag<String> startTag = new OutputTag<String>("start") {
        };
        OutputTag<String> displayTag = new OutputTag<String>("display") {
        };
        OutputTag<String> actionTag = new OutputTag<String>("action") {
        };
        OutputTag<String> errorTag = new OutputTag<String>("error") {
        };

        SingleOutputStreamOperator<String> pageDS = jsonObjWithNewFlagDS.process(new ProcessFunction<JSONObject, String>() {
            @Override
            public void processElement(JSONObject value, Context ctx, Collector<String> out) throws Exception {

                //尝试获取"err"字段
                String err = value.getString("err");
                if (err != null) {
                    ctx.output(errorTag, value.toJSONString());
                    value.remove("err");
                }

                //尝试获取"start"字段
                String start = value.getString("start");
                if (start != null) {
                    ctx.output(startTag, value.toJSONString());
                } else {

                    JSONObject common = value.getJSONObject("common");
                    JSONObject page = value.getJSONObject("page");
                    Long ts = value.getLong("ts");

                    //尝试获取"displays"
                    JSONArray displays = value.getJSONArray("displays");
                    if (displays != null) {
                        //遍历写出
                        for (int i = 0; i < displays.size(); i++) {
                            JSONObject display = displays.getJSONObject(i);
                            display.put("common", common);
                            display.put("page", page);
                            display.put("ts", ts);

                            //写出到曝光侧输出流中
                            ctx.output(displayTag, display.toJSONString());
                        }
                    }

                    //尝试获取"actions"
                    JSONArray actions = value.getJSONArray("actions");
                    if (actions != null) {
                        //遍历写出
                        for (int i = 0; i < actions.size(); i++) {
                            JSONObject action = actions.getJSONObject(i);
                            action.put("common", common);
                            action.put("page", page);

                            //写出到曝光侧输出流中
                            ctx.output(actionTag, action.toJSONString());
                        }
                    }

                    //移除曝光和动作数据
                    value.remove("displays");
                    value.remove("actions");

                    out.collect(value.toJSONString());
                }
            }
        });

        //TODO 7.提取各个流的数据，写入到kafka
        DataStream<String> startDS = pageDS.getSideOutput(startTag);
        DataStream<String> displayDS = pageDS.getSideOutput(displayTag);
        DataStream<String> actionDS = pageDS.getSideOutput(actionTag);
        DataStream<String> errorDS = pageDS.getSideOutput(errorTag);

        pageDS.print("Page>>>>");
        startDS.print("Start>>>>");
        displayDS.print("Display>>>>");
        actionDS.print("Action>>>>");
        errorDS.print("Error>>>");

        pageDS.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_traffic_page_log"));
        startDS.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_traffic_start_log"));
        displayDS.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_traffic_display_log"));
        actionDS.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_traffic_action_log"));
        errorDS.addSink(MyKafkaUtil.getFlinkKafkaProducer("dwd_traffic_error_log"));

        //TODO 8.启动任务
        env.execute("BaseLogApp");
    }
}
