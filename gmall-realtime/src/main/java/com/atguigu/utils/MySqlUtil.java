package com.atguigu.utils;

/**
 * @author ahao
 * @date 2022/7/22 16:25
 */
public class MySqlUtil {

    public static String getBaseDic() {
        return "CREATE TEMPORARY TABLE base_dic ( " +
                "  dic_code STRING, " +
                "  dic_name STRING, " +
                "  parent_code STRING, " +
                "  create_time STRING, " +
                "  operate_time STRING, " +
                "  PRIMARY KEY (dic_code) NOT ENFORCED " +
                ")" + getLookUpDDL("base_dic");
    }

    public static String getLookUpDDL(String table) {
        return " WITH ( " +
                "  'connector' = 'jdbc', " +
                "  'driver' = 'com.mysql.cj.jdbc.Driver', " +
                "  'lookup.cache.max-rows' = '10', " +
                "  'lookup.cache.ttl' = '1 hour', " +
                "  'url' = 'jdbc:mysql://hadoop102:3306/gmall', " +
                "  'table-name' = '" + table + "', " +
                "  'username' = 'root', " +
                "  'password' = '09061954' " +
                ")";
    }

}
