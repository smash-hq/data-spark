package com.wk.data.spark.infrastructure.util.replacing;

import org.apache.spark.sql.api.java.UDF2;

/**
 * @Author: smash_hq
 * @Date: 2021/11/29 15:23
 * @Description: null替换为指定值
 * @Version v1.0
 */

public class Null2SomeUdf<T> implements UDF2<T, String, String> {
    /**
     * functions.expr("null2Some(name,'')")
     *
     * @param t
     * @param s
     * @return
     * @throws Exception
     */
    @Override
    public String call(T t, String s) {
        if (t == null) {
            return s;
        }
        return t.toString();
    }
}
