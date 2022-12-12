package com.wk.data.spark.infrastructure.demo.dataset;

import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Author: smash_hq
 * @Date: 2021/9/30 10:14
 * @Description: sparkSession测试用例
 * @Version v1.0
 */

public class DemoSparkSession {
    public SparkSession demoSparkSession() {

        String date = LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE);
        return SparkSession.builder()
                .appName("demoSpark")
                .config("date", date)
                .config("spark.sql.debug.maxToStringFields", "100")
                .master("local[*]")
                .getOrCreate();
    }

}
