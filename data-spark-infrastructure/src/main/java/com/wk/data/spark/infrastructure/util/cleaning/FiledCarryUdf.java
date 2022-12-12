package com.wk.data.spark.infrastructure.util.cleaning;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF3;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * @Author: smash_hq
 * @Date: 2021/11/29 9:07
 * @Description: 舍位
 * @Version v1.0
 */

public class FiledCarryUdf implements UDF3<String, BigDecimal, Integer, String> {

    /**
     * @param value 操作字段
     * @param order 数量级
     * @param point 小数点
     * @return
     * @throws Exception
     */

    @Override
    public String call(String value, BigDecimal order, Integer point) {
        if (StringUtils.isBlank(value) || !StringUtils.isNumericSpace(value)) {
            return null;
        }
        BigDecimal bigDecimal = new BigDecimal(value);
        return bigDecimal.divide(order, point, RoundingMode.HALF_UP).toString();
    }

    public static void main(String[] args) {
        System.err.println(StringUtils.isNumericSpace(" "));
        BigDecimal bigDecimal = new BigDecimal("342");
        System.out.println(bigDecimal.divide(BigDecimal.valueOf(10), 2, RoundingMode.HALF_UP).toString());
        System.out.println(bigDecimal.divide(BigDecimal.valueOf(10), 2, RoundingMode.HALF_UP).toPlainString());
        double ss = 11.10;
        System.err.println(ss);
    }
}
