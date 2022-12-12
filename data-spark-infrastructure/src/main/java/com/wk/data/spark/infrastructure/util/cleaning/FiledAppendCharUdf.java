package com.wk.data.spark.infrastructure.util.cleaning;

import org.apache.spark.sql.api.java.UDF4;

/**
 * @Author: smash_hq
 * @Date: 2021/11/25 17:22
 * @Description: 指定字符串添加字符
 * @Version v1.0
 */

public class FiledAppendCharUdf implements UDF4<String, String, String, Integer, String> {
    /**
     * functions.expr("filedAppendChar(name,'char','0',-1)")
     *
     * @param value     操作字段
     * @param appendStr 填充的字符传
     * @param findStr   搜索的字符
     * @param i         0 在前面填充 1 在后面填充
     * @return
     */
    @Override
    public String call(String value, String appendStr, String findStr, Integer i) {
        if (value == null) {
            return null;
        }
        boolean flag = value.contains(findStr);
        if (flag) {
            return value.replace(findStr, i == 0 ? appendStr + findStr : findStr + appendStr);
        }
        return value;
    }
}
