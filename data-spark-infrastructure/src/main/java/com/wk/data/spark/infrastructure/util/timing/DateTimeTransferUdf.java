package com.wk.data.spark.infrastructure.util.timing;

import com.wk.data.spark.infrastructure.util.ValidateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF3;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @Author: smash_hq
 * @Date: 2021/11/30 15:34
 * @Description: 时间日期转换
 * @Version v1.0
 */

public class DateTimeTransferUdf implements UDF3<String, String, String, String> {
    /**
     * @param t
     * @param previous 之前的时间格式
     * @param later    之后的时间格式
     * @return
     * @throws Exception
     */
    @Override
    public String call(String t, String previous, String later) {
        if (StringUtils.isBlank(t) || !ValidateUtil.isDateTimeStr(t)) {
            return null;
        }
        try {
            LocalDateTime localDateTime = LocalDateTime.parse(t, DateTimeFormatter.ofPattern(previous));
            return localDateTime.format(DateTimeFormatter.ofPattern(later));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }


    public static void main(String[] args) {
        String dateTime = "2021-10-01 10:15:36";
        String previous = "yyyy-MM-dd HH:mm:ss";
        String later = "yyyyMMdd HH:mm:ss";
        LocalDateTime localDateTime = LocalDateTime.parse(dateTime, DateTimeFormatter.ofPattern(previous));
        String laterTime = localDateTime.format(DateTimeFormatter.ofPattern(later));
        System.out.println(laterTime);
    }

}
