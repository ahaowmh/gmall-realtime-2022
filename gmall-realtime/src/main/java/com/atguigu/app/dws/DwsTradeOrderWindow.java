package com.atguigu.app.dws;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TradeOrderBean;
import com.atguigu.utils.DateFormatUtil;
import com.atguigu.utils.MyClickHouseUtil;
import com.atguigu.utils.MyKafkaUtil;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import ru.yandex.clickhouse.ClickHouseUtil;

public class DwsTradeOrderWindow {
    public static void main(String[] args) throws Exception {

        //TODO 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //状态后端设置
        //        env.enableCheckpointing(3000L, CheckpointingMode.EXACTLY_ONCE);
        //        env.getCheckpointConfig().setCheckpointTimeout(60 * 1000L);
        //        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(3000L);
        //        env.getCheckpointConfig().enableExternalizedCheckpoints(
        //                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        //        );
        //        env.setRestartStrategy(RestartStrategies.failureRateRestart(
        //                10, Time.of(1L, TimeUnit.DAYS), Time.of(3L, TimeUnit.MINUTES)
        //        ));
        //        env.setStateBackend(new HashMapStateBackend());
        //        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/gmall/ck");
        //        System.setProperty("HADOOP_USER_NAME", "atguigu");

        //TODO 2.从 Kafka dwd_trade_order_detail 读取订单明细数据
        String topic = "dwd_trade_order_detail";
        String groupId = "dws_trade_order_window";
        DataStreamSource<String> kafkaDS = env.addSource(MyKafkaUtil.getFlinkKafkaConsumer(topic, groupId,"hadoop102:9092"));


        // TODO 3. 转换数据结构
        SingleOutputStreamOperator<String> filteredDS = kafkaDS.filter(
                new FilterFunction<String>() {

                    @Override
                    public boolean filter(String jsonStr) throws Exception {
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        String userId = jsonObj.getString("user_id");
                        String sourceTypeName = jsonObj.getString("source_type_name");
                        if (userId != null && sourceTypeName != null) {
                            return true;
                        }
                        return false;
                    }
                }
        );
        SingleOutputStreamOperator<JSONObject> mappedStream = filteredDS.map(JSON::parseObject);

        // TODO 4. 设置水位线
        SingleOutputStreamOperator<JSONObject> withWatermarkStream = mappedStream.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<JSONObject>forMonotonousTimestamps()
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<JSONObject>() {
                                    @Override
                                    public long extractTimestamp(JSONObject jsonObj, long recordTimestamp) {
                                        return jsonObj.getLong("ts") * 1000;
                                    }
                                }
                        )
        );

        // TODO 5. 按照UserId进行分组
        KeyedStream<JSONObject, String> keyedByUserIdStream = withWatermarkStream.keyBy(r -> r.getString("user_id"));

        // TODO 6. 统计当日下单独立用户数和新增下单用户数
        SingleOutputStreamOperator<TradeOrderBean> orderBeanStream = keyedByUserIdStream.process(
                new KeyedProcessFunction<String, JSONObject, TradeOrderBean>() {

                    private ValueState<String> lastOrderDtState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        lastOrderDtState = getRuntimeContext().getState(
                                new ValueStateDescriptor<String>("last_order_dt_state", String.class)
                        );
                    }

                    @Override
                    public void processElement(JSONObject jsonObj, Context ctx, Collector<TradeOrderBean> out) throws Exception {
                        String lastOrderDt = lastOrderDtState.value();

                        Long orderNewUserCount = 0L;
                        Long orderUniqueUserCount = 0L;
                        Long ts = jsonObj.getLong("ts") * 1000L;
                        String orderDt = DateFormatUtil.toDate(ts);

                        if (lastOrderDt == null) {
                            orderNewUserCount = 1L;
                            orderUniqueUserCount = 1L;
                            lastOrderDtState.update(orderDt);
                        } else {
                            if (!lastOrderDt.equals(orderDt)) {
                                orderUniqueUserCount = 1L;
                                lastOrderDtState.update(orderDt);
                            }
                        }

                        TradeOrderBean tradeOrderBean = new TradeOrderBean(
                                "",
                                "",
                                orderUniqueUserCount,
                                orderNewUserCount,
                                ts
                        );

                        out.collect(tradeOrderBean);
                    }
                }
        );

        // TODO 7. 开窗
        AllWindowedStream<TradeOrderBean, TimeWindow> windowDS = orderBeanStream.windowAll(TumblingEventTimeWindows.of(
                org.apache.flink.streaming.api.windowing.time.Time.seconds(10L)));

        // TODO 8. 聚合
        SingleOutputStreamOperator<TradeOrderBean> reducedStream = windowDS.reduce(
                new ReduceFunction<TradeOrderBean>() {
                    @Override
                    public TradeOrderBean reduce(TradeOrderBean value1, TradeOrderBean value2) throws Exception {
                        value1.setOrderNewUserCount(
                                value1.getOrderNewUserCount() + value2.getOrderNewUserCount());
                        value1.setOrderUniqueUserCount(
                                value1.getOrderUniqueUserCount() + value2.getOrderUniqueUserCount());
                        return value1;
                    }
                },
                new ProcessAllWindowFunction<TradeOrderBean, TradeOrderBean, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<TradeOrderBean> values, Collector<TradeOrderBean> out) throws Exception {
                        String stt = DateFormatUtil.toYmdHms(context.window().getStart());
                        String edt = DateFormatUtil.toYmdHms(context.window().getEnd());

                        for (TradeOrderBean value : values) {
                            value.setStt(stt);
                            value.setEdt(edt);
                            value.setTs(System.currentTimeMillis());
                            out.collect(value);
                        }
                    }
                }
        );

        // TODO 9. 写出到 ClickHouse
        reducedStream.print(">>>>>>>>>");
        reducedStream.addSink(MyClickHouseUtil.getSinkFunction("insert into dws_trade_order_window values(?,?,?,?,?)"));

        //TODO 10.启动任务
        env.execute("DwsTradeOrderWindow");
    }
}
