package com.wk.data.spark.infrastructure.util.cleaning;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF2;

/**
 * @Author: smash_hq
 * @Date: 2021/12/7 9:33
 * @Description: 删除空格
 * @Version v1.0
 */

public class FiledDeleteBlankUdf implements UDF2<String, Integer, String> {
    /**
     * @param value
     * @param pos   0-删除首尾空格 1-删除所有空格 2-任意
     * @return
     * @throws Exception
     */
    @Override
    public String call(String value, Integer pos) {
        if (value == null) {
            return null;
        }
        switch (pos) {
            case 0:
                return value.trim();
            case 1:
            case 2:
                return StringUtils.deleteWhitespace(value);
            default:
                return value;
        }
    }
}
