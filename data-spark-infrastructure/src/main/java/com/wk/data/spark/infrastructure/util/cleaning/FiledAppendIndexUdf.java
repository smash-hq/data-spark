package com.wk.data.spark.infrastructure.util.cleaning;


import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF3;

/**
 * @Author: smash_hq
 * @Date: 2021/11/25 16:54
 * @Description: 在指定位置添加字符串
 * @Version v1.0
 */

public class FiledAppendIndexUdf implements UDF3<String, String, Integer, String> {

    /**
     * functions.expr("filedAppend(name,'append',1)")
     *
     * @param value 操作字段
     * @param str   填充的字符
     * @param pos   0-添加在开头 1-添加在结尾
     * @return
     */
    @Override
    public String call(String value, String str, Integer pos) {
        if (value == null || StringUtils.isBlank(str)) {
            return value;
        }
        return pos == 0 ? str + value : value + str;
    }
}
