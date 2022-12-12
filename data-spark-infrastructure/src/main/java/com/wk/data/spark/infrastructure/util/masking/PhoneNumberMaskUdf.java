package com.wk.data.spark.infrastructure.util.masking;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

/**
 * @Author: smash_hq
 * @Date: 2021/12/1 10:32
 * @Description: 电话号码脱敏
 * @Version v1.0
 */

public class PhoneNumberMaskUdf implements UDF1<String, String> {

    /**
     * 保留前三位和后三位
     *
     * @param number
     * @return
     */
    @Override
    public String call(String number) {
        if (StringUtils.isBlank(number)) {
            return number;
        }
        int length = number.length();
        int numLength = 11;
        if (length >= numLength) {
            number = number.replaceAll("(\\w{3})(\\w*)(\\w{3})", "$1*****$3");
        }
        return number;
    }

    public static void main(String[] args) {
        String number = "18066583797";
        System.out.println(number.replaceAll("(\\w{3})(\\w*)(\\w{3})", "$1*****$3"));
    }
}
