package com.wk.data.spark.service.quality.executor;

/**
 * @Author: smash_hq
 * @Date: 2021/8/26 10:11
 * @Description: 数据质量规则校验分析
 * @Version v1.0
 */
public interface QualityRuleService {

    /**
     * 数据质量规则校验分析
     *
     * @param args
     * @throws Exception
     */
    void qualityRules(String... args) throws Exception;

}
