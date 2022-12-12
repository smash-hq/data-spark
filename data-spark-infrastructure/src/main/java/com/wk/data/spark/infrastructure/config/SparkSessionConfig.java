package com.wk.data.spark.infrastructure.config;

import org.apache.spark.sql.SparkSession;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * @Author: gwl
 * @Description:
 * @Date: 2021-08-23 10:21:23
 */

@Component
public class SparkSessionConfig {

    @Value("${spark.master:local[*]}")
    private String master;

    @Bean
    public SparkSession sparkSession() {

        String date = LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE);
        return SparkSession.builder()
                .config("date", date)
                .config("spark.sql.debug.maxToStringFields", "100")
                .master(master)
                .getOrCreate();
    }
}
