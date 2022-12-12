package com.wk.data.spark.infrastructure.util.quality;

import com.alibaba.fastjson.JSON;
import com.vcolco.components.basis.exception.BizException;
import org.apache.spark.sql.api.java.UDF6;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @Author: smash_hq
 * @Date: 2022/1/5 10:31
 * @Description: 时间判断
 * @Version v1.0
 */

public class TimeLogicCheckUdf<T, R> implements UDF6<T, R, Long, String, String, String, String> {

    @Override
    public String call(T t, R r, Long rank, String s, String handleCode, String beCompareCode) throws Exception {
        List<CaseOpera> list = new ArrayList<>();
        if (t == null || r == null) {
            list.add(CaseOpera.builder().rowNumberRank(rank).code(handleCode + "," + beCompareCode).type("time").judge(true).build());
            return JSON.toJSONString(list);
        }
        boolean result = false;
        if (!t.getClass().equals(r.getClass())) {
            throw new BizException(t.getClass() + "和" + r.getClass() + "类型比较不一致!");
        }
        if (t instanceof Long || t instanceof Integer) {
            long i = (Long) t;
            long j = (Long) r;
            if (Long.toString(i).length() == 10) {
                i = i * 1000;
            }
            if (Long.toString(j).length() == 10) {
                j = j * 1000;
            }
            if (">".equals(s)) {
                result = i > j;
            } else if ("<".equals(s)) {
                result = i < j;
            } else {
                LocalDate handle = Instant.ofEpochMilli(i).atZone(ZoneOffset.ofHours(8)).toLocalDate();
                LocalDate beCompare = Instant.ofEpochMilli(j).atZone(ZoneOffset.ofHours(8)).toLocalDate();
                result = handle.getDayOfYear() == beCompare.getDayOfYear();
            }
        } else if (t instanceof Date) {
            LocalDateTime handle = LocalDateTime.ofInstant(((Date) t).toInstant(), ZoneOffset.ofHours(8));
            LocalDateTime beCompare = LocalDateTime.ofInstant(((Date) r).toInstant(), ZoneOffset.ofHours(8));
            if (">".equals(s)) {
                result = handle.atZone(ZoneOffset.ofHours(8)).toInstant().toEpochMilli() > beCompare.atZone(ZoneOffset.ofHours(8)).toInstant().toEpochMilli();
            } else if ("<".equals(s)) {
                result = handle.atZone(ZoneOffset.ofHours(8)).toInstant().toEpochMilli() < beCompare.atZone(ZoneOffset.ofHours(8)).toInstant().toEpochMilli();
            } else {
                result = handle.getDayOfYear() == beCompare.getDayOfYear();
            }
        }
        if (!result) {
            list.add(CaseOpera.builder().rowNumberRank(rank).code(handleCode + "," + beCompareCode).type("time").judge(true).build());
        }
        if (list.isEmpty()) {
            return null;
        }
        return JSON.toJSONString(list);
    }

    public static void main(String[] args) {
        Long a = 1000L;
        Long b = 1000L;
        System.out.println(a.getClass().equals(b.getClass()));

    }

}
