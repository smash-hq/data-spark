package com.wk.data.spark.infrastructure.util.cleaning;

import org.apache.spark.sql.api.java.UDF3;

/**
 * @Author: smash_hq
 * @Date: 2021/11/26 14:47
 * @Description: 删除指定字符串
 * @Version v1.0
 */

public class FiledDeleteChar implements UDF3<String, String, Integer, String> {
    /**
     * functions.expr("filedDeleteChar(name,'0',0)")
     *
     * @param value   操作字段
     * @param findStr 搜索字符串
     * @param pos     0 前 1 后 2 任意位置
     * @return
     */
    @Override
    public String call(String value, String findStr, Integer pos) {
        if (value == null) {
            return null;
        }
        String col = value;
        switch (pos) {
            case 0:
                if (col.startsWith(findStr)) {
                    return col.substring(findStr.length());
                }
                return col;
            case 1:
                if (col.endsWith(findStr)) {
                    return col.substring(0, col.length() - findStr.length());
                }
                return col;
            case 2:
                if (col.contains(findStr)) {
                    return col.replace(findStr, "");
                }
                return col;
            default:
                return col;
        }
    }
}
