package com.wk.data.mdm;

import com.wk.data.spark.service.mdm.executor.MdmService;
import com.wk.data.spark.service.mdm.executor.MdmTempService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

/**
 * @author
 * @Author: smash_hq
 * @Date: 2021/12/3 10:45
 * @Description: 主数据管理
 * @Version v1.0
 */

@SpringBootApplication(scanBasePackages = {"com.wk.data.spark"})
@EnableFeignClients(basePackages = {"com.wk.data.etl.facade", "com.wk.data.share.facade"})
@EnableDiscoveryClient
public class MasterDataApplication implements CommandLineRunner {


    @Autowired
    private MdmService mdmService;
    @Autowired
    private MdmTempService mdmTemp;


    public static void main(String[] args) {
        int flag = 0;
        try {
            System.setProperty("nacos.logging.default.config.enabled", "false");
            SpringApplication.run(MasterDataApplication.class, args);
        } catch (Exception e) {
            flag += 1;
            throw e;
        } finally {
            System.exit(flag);
        }
    }

    @Override
    public void run(String... args) throws Exception {

        mdmService.masterDataManagement(args);
    }
}