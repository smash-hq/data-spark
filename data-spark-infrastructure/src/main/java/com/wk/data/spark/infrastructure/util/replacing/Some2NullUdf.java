package com.wk.data.spark.infrastructure.util.replacing;

import org.apache.spark.sql.api.java.UDF2;

/**
 * @Author: smash_hq
 * @Date: 2021/11/29 15:22
 * @Description: 指定值替换为null
 * @Version v1.0
 */

public class Some2NullUdf implements UDF2<String, String, String> {
    /**
     * functions.expr("some2NullUDF(name,'')")
     *
     * @param value
     * @param spev
     * @return
     * @throws Exception
     */
    @Override
    public String call(String value, String spev) throws Exception {
        if (value == null) {
            return null;
        }
        return spev.equals(value) ? null : value;
    }
}
