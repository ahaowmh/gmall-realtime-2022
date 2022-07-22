package com.atguigu.bean;

/**
 * @author ahao
 * @date 2022/7/18 23:29
 */
import lombok.Data;

@Data
public class TableProcess {
    //来源表
    String sourceTable;
    //输出表
    String sinkTable;
    //输出字段
    String sinkColumns;
    //主键字段
    String sinkPk;
    //建表扩展
    String sinkExtend;
}
