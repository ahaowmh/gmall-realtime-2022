package com.atguigu.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Properties;

/**
 * @author ahao
 * @date 2022/7/18 21:51
 */
public class MyKafkaUtil {

    public static FlinkKafkaConsumer<String> getFlinkKafkaConsumer(String topic,String groupId,String servers){
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,servers);
        return new FlinkKafkaConsumer<String>(
                topic,
                new KafkaDeserializationSchema<String>() {
                    @Override
                    public boolean isEndOfStream(String nextElement) {
                        return false;
                    }

                    @Override
                    public String deserialize(ConsumerRecord<byte[], byte[]> record) throws Exception {
                        if (record == null || record.value() == null) {
                            return null;
                        } else {
                            return new String(record.value());
                        }
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return BasicTypeInfo.STRING_TYPE_INFO;
                    }
                },
                properties);
    }

    public static FlinkKafkaProducer<String> getFlinkKafkaProducer(String topic) {
        return new FlinkKafkaProducer<String>("hadoop102:9092",
                topic,
                new SimpleStringSchema());
    }


    public static String getTopicDb(String topic,String groupId ) {
        return "CREATE TABLE topic_db ( " +
                "  `database` STRING, " +
                "  `table` STRING, " +
                "  `type` STRING, " +
                "  `ts` BIGINT, " +
                "  `data` Map<STRING,STRING>, " +
                "  `old` Map<STRING,STRING>, " +
                "  `pt` AS PROCTIME() " +
                ")" + getKafkaDDL(topic, groupId);
    }

    public static String getKafkaDDL(String topic, String groupId) {
        return " WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'properties.group.id' = '" + groupId + "', " +
                "  'scan.startup.mode' = 'latest-offset', " +
                "  'format' = 'json' " +
                ")";
    }

    public static String getSinkKafkaDDL(String topic) {
        return " WITH ( " +
                "  'connector' = 'kafka', " +
                "  'topic' = '" + topic + "', " +
                "  'properties.bootstrap.servers' = 'hadoop102:9092', " +
                "  'format' = 'json' " +
                ")";
    }
}