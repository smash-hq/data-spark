package com.wk.data.spark.infrastructure.util.converting;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF2;

/**
 * @Author: smash_hq
 * @Date: 2021/12/2 13:32
 * @Description: 多行测试
 * @Version v1.0
 */

public class MoreColUdf implements UDF2<String, String, String> {
    @Override
    public String call(String s, String s2) {
        if (StringUtils.isEmpty(s)) {
            return s2;
        }
        return s;
    }
}
