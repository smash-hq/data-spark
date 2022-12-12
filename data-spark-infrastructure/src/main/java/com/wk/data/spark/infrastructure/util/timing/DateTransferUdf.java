package com.wk.data.spark.infrastructure.util.timing;

import com.wk.data.spark.infrastructure.util.ValidateUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF3;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Author: smash_hq
 * @Date: 2021/11/30 15:46
 * @Description: 日期转换
 * @Version v1.0
 */

public class DateTransferUdf implements UDF3<String, String, String, String> {
    /**
     * @param t
     * @param previous 之前的时间格式
     * @param later    之后的时间格式
     * @return
     * @throws Exception
     */
    @Override
    public String call(String t, String previous, String later) {
        if (StringUtils.isBlank(t) || !ValidateUtil.isDateStr(t)) {
            return null;
        }
        try {
            LocalDate localDate = LocalDate.parse(t, DateTimeFormatter.ofPattern(previous));
            return localDate.format(DateTimeFormatter.ofPattern(later));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static void main(String[] args) {
        String dateTime = "2021-10-01";
        String previous = "yyyy-MM-dd";
        String later = "yyyyMMdd";
        LocalDate localDate = LocalDate.parse(dateTime, DateTimeFormatter.ofPattern(previous));
        String laterTime = localDate.format(DateTimeFormatter.ofPattern(later));
        System.out.println(laterTime);
    }

}
