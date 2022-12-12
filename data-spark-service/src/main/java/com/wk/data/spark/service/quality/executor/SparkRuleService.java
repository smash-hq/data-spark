package com.wk.data.spark.service.quality.executor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * @Author: smash_hq
 * @Date: 2021/8/31 14:05
 * @Description: spark操作
 * @Version v1.0
 */
public interface SparkRuleService {

    /**
     * 校验唯一性
     *
     * @param filedCode
     * @return
     */
    Dataset<Row> unique(String filedCode);

    /**
     * 校验完整性
     *
     * @param filedCode
     * @return
     */
    Dataset<Row> integrality(String filedCode);

    /**
     * 校验合法性（字符串）
     *
     * @param minString
     * @param maxString
     * @param filedCode
     * @return
     */
    Dataset<Row> legalString(Integer minString, Integer maxString, String filedCode);

    /**
     * 校验准确性（数值）
     *
     * @param minValue
     * @param maxValue
     * @param filedCode
     * @return
     */
    Dataset<Row> exactValue(Double minValue, Double maxValue, String filedCode);

    /**
     * 校验准确性（字符串）
     *
     * @param checkRule
     * @param filedCode
     * @return
     */
    Dataset<Row> regularRule(String checkRule, String filedCode);
}
