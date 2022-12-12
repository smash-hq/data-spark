package com.wk.data.spark.service.mdm.executor;

/**
 * @Author: smash_hq
 * @Date: 2021/12/3 10:50
 * @Description: 主数据管理
 * @Version v1.0
 */
public interface MdmService {

    /**
     * 主数据管理分析
     *
     * @param args
     * @throws Exception
     */
    void masterDataManagement(String... args) throws Exception;
}
