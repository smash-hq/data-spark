package com.wk.data.spark.service.quality.executor.impl;

import com.wk.data.spark.infrastructure.util.cleaning.RegularRule;
import com.wk.data.spark.service.quality.executor.SparkRuleService;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;

/**
 * @Author: smash_hq
 * @Date: 2021/8/31 14:06
 * @Description: spark操作
 * @Version v1.0
 */
@Component
public final class SparkRuleServiceImpl implements SparkRuleService, Serializable, scala.Serializable {
    @Autowired
    private SparkSession spark;

    @Override
    public Dataset<Row> unique(String filedCode) {
        Dataset<Row> dataset = spark.sql("SELECT * FROM checkData t2 RIGHT JOIN (SELECT t0." + filedCode + " AS tmp_code FROM (SELECT " + filedCode + ",COUNT(1) AS amount FROM checkData GROUP BY " + filedCode + " )t0 WHERE t0.amount > 1) t1 ON t1.tmp_code=t2." + filedCode + " WHERE tmp_code IS NOT NULL ");
        dataset = dataset.drop("tmp_code");
        return dataset;
    }

    @Override
    public Dataset<Row> integrality(String filedCode) {
        Dataset<Row> dataset = spark.sql("SELECT * FROM checkData WHERE " + filedCode + " IS NULL OR " + filedCode + " = ''");
        // 防止 mongo 存储时为空字段不可见，设置为 null 字符串的方式显示
        dataset = dataset.withColumn(filedCode, functions.lit("null"));
        return dataset;
    }

    @Override
    public Dataset<Row> legalString(Integer minString, Integer maxString, String filedCode) {
        Dataset<Row> dataset;
        if (minString == null && maxString != null) {
            StringBuilder s = new StringBuilder();
            dataset = spark.sql("SELECT * FROM checkData WHERE LENGTH(" + filedCode + ") > " + maxString);
            return dataset;
        } else if (minString != null && maxString == null) {
            dataset = spark.sql("SELECT * FROM checkData WHERE LENGTH(" + filedCode + ") < " + minString);
            return dataset;
        }
        dataset = spark.sql("SELECT * FROM checkData WHERE LENGTH(" + filedCode + ") < " + minString + " OR LENGTH(" + filedCode + ") > " + maxString);
        return dataset;
    }

    @Override
    public Dataset<Row> exactValue(Double minValue, Double maxValue, String filedCode) {
        Dataset<Row> dataset;
        if (minValue == null && maxValue != null) {
            dataset = spark.sql("SELECT * FROM checkData WHERE " + filedCode + " > " + maxValue);
            return dataset;
        } else if (minValue != null && maxValue == null) {
            dataset = spark.sql("SELECT * FROM checkData WHERE " + filedCode + " < " + minValue);
            return dataset;
        }
        dataset = spark.sql("SELECT * FROM checkData WHERE " + filedCode + " < " + minValue + " OR " + filedCode + " > " + maxValue);
        return dataset;
    }

    @Override
    public Dataset<Row> regularRule(String checkRule, String filedCode) {
        Dataset<Row> dataset;
        UDF2<Integer, String, Boolean> udf2 = new RegularRule();
        spark.udf().register("regularRule", udf2, DataTypes.BooleanType);
        dataset = spark.sql("SELECT * FROM (SELECT *, regularRule(" + filedCode + ", '" + checkRule + "')" + " AS verify FROM checkData)t0 WHERE t0.verify = false");
        dataset = dataset.drop("verify");
        return dataset;
    }

}
