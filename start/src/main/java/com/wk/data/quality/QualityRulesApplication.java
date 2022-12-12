package com.wk.data.quality;

import com.wk.data.spark.service.quality.executor.QualityRuleService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * @Author: smash_hq
 * @Date: 2021/8/26 10:11
 * @Description: 数据质量规则校验分析
 * @Version v1.0
 */
@SpringBootApplication(scanBasePackages = {"com.wk.data.spark"})
@EnableFeignClients(basePackages = {"com.wk.data.etl.facade", "com.wk.data.share.facade"})
@EnableDiscoveryClient
public class QualityRulesApplication implements CommandLineRunner {

    @Autowired
    private QualityRuleService qualityRuleService;

    public static void main(String[] args) {
        try {
            System.setProperty("nacos.logging.default.config.enabled", "false");
            SpringApplication.run(QualityRulesApplication.class, args);
        } finally {
            System.exit(0);
        }
    }


    @Override
    public void run(String... args) throws Exception {
        qualityRuleService.qualityRules(args);
    }
}