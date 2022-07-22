package com.atguigu.app.func;

import com.alibaba.druid.pool.DruidDataSource;
import com.alibaba.druid.pool.DruidPooledConnection;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.bean.TableProcess;
import com.atguigu.common.GmallConfig;
import com.atguigu.utils.DruidDSUtil;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

/**
 * @author ahao
 * @date 2022/7/19 10:37
 */
public class TableProcessFunction extends BroadcastProcessFunction<JSONObject,String,JSONObject> {

    //构造方法传参
    private MapStateDescriptor<String, TableProcess> stateDescriptor;

    public TableProcessFunction(MapStateDescriptor<String, TableProcess> stateDescriptor) {
        this.stateDescriptor = stateDescriptor;
    }

    public TableProcessFunction() {
    }

    //初始化连接池，每个并行度创建一个连接（连接不可序列化）
    private DruidDataSource druidDataSource;
    @Override
    public void open(Configuration parameters) throws Exception {
        druidDataSource = DruidDSUtil.createDataSource();
    }

    //处理广播流
    @Override
    public void processBroadcastElement(String value, BroadcastProcessFunction<JSONObject, String, JSONObject>.Context ctx, Collector<JSONObject> out) throws Exception {
        //数据是通过FlinkCDC从TableProcess这张表拿到  大概格式如下：
        //Value:{"before":null,"after":{"id":6,"tm_name":"长粒香","logo_url":"/static/default.jpg"},
        // "source":{"version":"1.5.4.Final","connector":"mysql","name":"mysql_binlog_source","ts_ms":1658192885916,
        // "snapshot":"false","db":"gmall-211227-flink","sequence":null,"table":"base_trademark","server_id":0,"gtid":null,
        // "file":"","pos":0,"row":0,"thread":null,"query":null},"op":"r","ts_ms":1658192885916,"transaction":null}
        // 1、获取数据并转换为Javabean（TableProcess)
        JSONObject jsonObject = JSON.parseObject(value);
        TableProcess tableProcess = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);

        // 2、建表（维度表）
        checkTable(tableProcess.getSinkTable(),
                tableProcess.getSinkColumns(),
                tableProcess.getSinkPk(),
                tableProcess.getSinkExtend());

        // 3、写入状态（会自动发送到广播流）
        String key = tableProcess.getSourceTable();
        //getBroadcastState需要的参数MapStateDscriptor在DimApp类中定义过，可以通过构造方法传参获取！！！
        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);
        broadcastState.put(key,tableProcess);

    }
    //校验并建表  在此之前，需要获取Phoenix驱动、获取连接----->GmallConfig工具类、德鲁伊连接池
    //建表语句格式：create table if not exists db.tablename(id varchar primary key,name varchar,sex varchar) xxx
    private void checkTable(String sinkTable, String sinkColumns, String sinkPk, String sinkExtend) {
        DruidPooledConnection connection = null;
        PreparedStatement preparedStatement = null;
        try {
            if (sinkPk == null){
                sinkPk = "id";
            }
            if (sinkExtend == null){
                sinkExtend = "";
            }
            //根据格式拼接建表SQL语句
            StringBuilder sql = new StringBuilder("create table if not exists ")
                    .append(GmallConfig.HBASE_SCHEMA)
                    .append(".")
                    .append(sinkTable)
                    .append("(");
            //sinkColumns输出字段为长字符串，需要切割循环判断是否为主键
            String[] columns = sinkColumns.split(",");

            for (int i = 0; i < columns.length; i++) {

                String column = columns[i];
                if (sinkPk.equals(column)) {
                    sql.append(column).append(" varchar primary key");
                } else {
                    sql.append(column).append(" varchar");
                }
                //除了最后一个字段其他字段后面都要拼接 ,
                if (i < columns.length - 1) {
                    sql.append(",");
                }
            }
            sql.append(")").append(sinkExtend);

            //打印SQL语句，用于测试
            System.out.println(sql);

            //获取连接，编译SQL
            //编译阶段可能存在异常，此时必须抛出运行时异常，终止程序，否者建表失败，后续数据不能根据维度表将数据写出
            connection = druidDataSource.getConnection();
            preparedStatement = connection.prepareStatement(sql.toString());

            //执行SQL，写入数据
            preparedStatement.execute();

        } catch (SQLException e) {
            throw new RuntimeException("创建" + sinkTable + "失败。。。");
        } finally {
            //释放资源
            if (preparedStatement != null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //处理主流
    @Override
    public void processElement(JSONObject value, BroadcastProcessFunction<JSONObject, String, JSONObject>.ReadOnlyContext ctx, Collector<JSONObject> out) throws Exception {

        // 1、获取广播数据   数据来源：Maxwell
        // value:{"database":"gmall","table":"cart_info","type":"update","ts":1592270938,"xid":13090,"xoffset":1573,"data":{"id":100924,"user_id":"93","sku_id":16,"cart_price":4488,
        // "sku_num":1,"img_url":"http://47.93.148.192:8080/group1/M0rBHu8l-sklaALrngAAHGDqdpFtU741.jpg","sku_name":"华为 HUAWEI P40 麒麟990 5G SoC芯片 5000万30倍数字变焦 8GB+128GB亮黑色全网通5G手机",
        // "is_checked":null,"create_time":"2020-06-14 09:28:57","operate_time":null,"is_ordered":1,"order_time":"2021-10-17 09:28:58","source_type":"2401","source_id":null},
        // "old":{"is_ordered":0,"order_time":null}}
        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(stateDescriptor);
        TableProcess tableProcess = broadcastState.get(value.getString("table"));

        // 2、过滤数据 行
        String type = value.getString("type");
        if (tableProcess != null && ("insert".equals(type) || "update".equals(type) || "bootstrap-insert".equals(type))) {
            //过滤数据 列
            filterColumns(value.getJSONObject("data"),tableProcess.getSinkColumns());


            // 3、补充sinkTable字段，写出数据到Phoenix
            value.put("sinkTable",tableProcess.getSinkTable());
            out.collect(value);

        }
    }

    //过滤数据 列
    private void filterColumns(JSONObject data, String sinkColumns) {

        //将sinkcolumns(字符串，不处理会有字段名包含问题）转换为数组（数组没有contains方法），再转换为list集合
        String[] columns = sinkColumns.split(",");
        List<String> columnList = Arrays.asList(columns);

        /*//遍历data（map,用entryset遍历，整行数据），看是否属于sinkcolumns
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        Iterator<Map.Entry<String, Object>> iterator = entries.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> next = iterator.next();
            if (!columnList.contains(next.getKey())) {
                //移除整行
                iterator.remove();
            }
        }*/

        //idea自动生成，原格式见上
        //遍历data（map,用entryset遍历，整行数据），看是否属于sinkcolumns
        Set<Map.Entry<String, Object>> entries = data.entrySet();
        //移除整行
        entries.removeIf(next -> !columnList.contains(next.getKey()));
    }

}
