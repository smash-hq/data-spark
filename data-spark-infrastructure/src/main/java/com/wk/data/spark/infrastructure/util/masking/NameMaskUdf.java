package com.wk.data.spark.infrastructure.util.masking;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

/**
 * @Author: smash_hq
 * @Date: 2021/12/1 10:55
 * @Description: 中文名脱敏
 * @Version v1.0
 */

public class NameMaskUdf implements UDF1<String, String> {

    /**
     * 除了第一位均替换为 *
     *
     * @param name
     * @return
     */
    @Override
    public String call(String name) {
        if (StringUtils.isBlank(name)) {
            return name;
        }
        StringBuilder sb = new StringBuilder();
        int length = name.length();
        int sindex = length >= 4 ? 2 : 1;
        sb.append(name, 0, sindex);
        for (int i = sindex; i < length; i++) {
            sb.append("*");
        }
        return sb.toString();
    }
}
