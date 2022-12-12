package com.wk.data.spark.infrastructure.util.timing;

import com.wk.data.spark.infrastructure.util.ValidateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF3;

import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * @Author: smash_hq
 * @Date: 2021/11/30 15:48
 * @Description: 时间转换
 * @Version v1.0
 */

public class TimeTransferUdf implements UDF3<String, String, String, String> {
    /**
     * @param t
     * @param previous 之前的时间格式
     * @param later    之后的时间格式
     * @return
     * @throws Exception
     */
    @Override
    public String call(String t, String previous, String later) {
        if (StringUtils.isBlank(t) || !ValidateUtil.isTimeStr(t)) {
            return null;
        }
        try {
            LocalTime localTime = LocalTime.parse(t, DateTimeFormatter.ofPattern(previous));
            return localTime.format(DateTimeFormatter.ofPattern(later));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        String dateTime = "10:25:36";
        String previous = "HH:mm:ss";
        String later = "HHmmss";
        System.err.println(LocalTime.parse("122323", DateTimeFormatter.ofPattern(later)));
        LocalTime localTime = LocalTime.parse(dateTime, DateTimeFormatter.ofPattern(previous));
        String laterTime = localTime.format(DateTimeFormatter.ofPattern(later));
        System.out.println(laterTime);
    }


}