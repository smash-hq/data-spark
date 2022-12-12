package com.wk.data.spark.infrastructure.util.converting;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.api.java.UDF4;

/**
 * @Author: smash_hq
 * @Date: 2021/11/29 15:19
 * @Description: 字段合并
 * @Version v1.0
 */

public class FiledMergeUdf<T> implements UDF4<T, Column, Column, Column, String> {

    @Override
    public String call(T t, Column column, Column column2, Column column3) throws Exception {
        return null;
    }
}
