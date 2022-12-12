package com.wk.data.govern;

import com.wk.data.spark.service.govern.executor.GovernService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

import java.util.Arrays;

/**
 * @program: data-spark-job
 * @description: 数据清洗任务入口
 * @author: gwl
 * @create: 2021-09-09 16:40
 **/
@EnableFeignClients(basePackages = {"com.wk.data.etl.facade", "com.wk.data.share.facade"})
@SpringBootApplication(scanBasePackages = {"com.wk.data.spark"})
@EnableDiscoveryClient
public class DataCleanApplication implements CommandLineRunner {

    @Autowired
    private GovernService governService;

    public static void main(String[] args) {
        int status = 0;
        try {
            System.setProperty("nacos.logging.default.config.enabled", "false");
            SpringApplication.run(DataCleanApplication.class, args);
        } catch (Exception e) {
            status = 1;
        } finally {
            System.exit(status);
        }
    }

    @Override
    public void run(String... args) throws Exception {
        System.err.println("数据清洗的任务ID:" + Arrays.toString(args));
        governService.startCleanData(args);
    }
}