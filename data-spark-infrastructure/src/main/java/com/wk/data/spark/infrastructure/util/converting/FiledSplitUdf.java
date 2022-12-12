package com.wk.data.spark.infrastructure.util.converting;

import org.apache.spark.sql.api.java.UDF3;

import java.util.Arrays;
import java.util.List;

/**
 * @Author: smash_hq
 * @Date: 2021/11/18 11:29
 * @Description: 按分隔符拆分字段的函数
 * @Version v1.0
 */

public class FiledSplitUdf implements UDF3<String, String, Integer, String> {

    @Override
    public String call(String s, String s2, Integer i) {
        String[] strings = s.split(s2, i);
        List<String> list = Arrays.asList(strings);
        return String.join(",", list);
    }
}
