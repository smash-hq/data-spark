package com.wk.data.spark.infrastructure.util.cleaning;

import org.apache.spark.sql.api.java.UDF2;

/**
 * @Author: smash_hq
 * @Date: 2021/9/8 10:17
 * @Description: 正则校验自定义函数
 * @Version v1.0
 */
public class RegularRule<T> implements UDF2<T, String, Boolean> {

    @Override
    public Boolean call(T t, String s) {
        return t.toString().matches(s);
    }
}
