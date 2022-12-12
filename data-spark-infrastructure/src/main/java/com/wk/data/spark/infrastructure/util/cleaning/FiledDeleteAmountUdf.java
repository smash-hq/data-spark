package com.wk.data.spark.infrastructure.util.cleaning;

import org.apache.spark.sql.api.java.UDF3;

/**
 * @Author: smash_hq
 * @Date: 2021/11/26 14:38
 * @Description: 删除指定数量的字符
 * @Version v1.0
 */

public class FiledDeleteAmountUdf implements UDF3<String, Integer, Integer, String> {
    /**
     * functions.expr("filedDeleteAmount(name,5,-1)")
     *
     * @param value  操作字段
     * @param amount 删除数量
     * @param pos    0 从前 1 从后
     * @return
     */
    @Override
    public String call(String value, Integer amount, Integer pos) {
        if (value == null) {
            return null;
        }
        int length = value.length();
        switch (pos) {
            case 0:
                return amount >= length ? null : value.substring(amount);
            case 1:
                return amount >= length ? null : value.substring(0, value.length() - amount);
            default:
                return value;
        }
    }
}
