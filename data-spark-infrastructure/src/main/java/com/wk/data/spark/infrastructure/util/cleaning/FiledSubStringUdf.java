package com.wk.data.spark.infrastructure.util.cleaning;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF4;

/**
 * @Created: smash_hq at 11:34 2022/9/27
 * @Description: 字符串截取函数
 */

public class FiledSubStringUdf implements UDF4<String, Integer, Integer, Integer, String> {
    @Override
    public String call(String value, Integer mode, Integer start, Integer end) throws Exception {
        if (StringUtils.isBlank(value)) {
            return value;
        }
        int length = value.length();
        // 超出长度则取字符串长度
        if (end != null && length < end) {
            end = length;
        }
        if (mode == 0) {
            value = leftToRightSubString(start, end, value);
        } else if (mode == 1) {
            // 反转字符串
            value = new StringBuffer(value).reverse().toString();
            value = leftToRightSubString(start, end, value);
            value = new StringBuffer(value).reverse().toString();
        }
        return value;
    }

    private static String leftToRightSubString(Integer start, Integer end, String value) {
        if (start != null) {
            if (end != null) {
                value = value.substring(start, end);
            } else {
                value = value.substring(start);
            }
        } else {
            if (end != null) {
                value = value.substring(0, end);
            }
        }
        return value;
    }

}
