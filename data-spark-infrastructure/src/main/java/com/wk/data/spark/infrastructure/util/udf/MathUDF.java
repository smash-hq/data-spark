package com.wk.data.spark.infrastructure.util.udf;


import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.api.java.UDF3;

import java.math.BigDecimal;

/**
 * @program: data-spark-job
 * @description: 数字加减乘除BigDecimal.ROUND_UP
 * @author: gwl
 * @create: 2022-07-27 10:07
 **/
public class MathUDF {

    public static UDF3<Number, Number, Integer, String> subtract() {
        return (x, y, z) -> {
            if (x == null || y == null) {
                return null;
            }
            BigDecimal decimal = new BigDecimal(String.valueOf(x)).subtract(new BigDecimal(String.valueOf(y)));
            if (z != null) {
                decimal = decimal.setScale(Math.abs(z), BigDecimal.ROUND_HALF_UP);
            }
            return decimal.toString();
        };
    }

    public static UDF2<Integer, Integer, Integer> addExact() {
        return (x, y) -> x + y;
    }

    public static UDF2<Integer, Integer, Integer> multiply() {
        return (x, y) -> x * y;
    }

}
