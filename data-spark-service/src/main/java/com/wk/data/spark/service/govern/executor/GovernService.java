package com.wk.data.spark.service.govern.executor;

/**
 * @program: data-spark-job
 * @description: 数据治理逻辑
 * @author: gwl
 * @create: 2021-09-16 14:14
 **/
public interface GovernService {

    /**
     * 开始执行数据清洗任务
     *
     * @param args
     */
    void startCleanData(String[] args) throws Exception;

    /**
     * 开始执行数据拆分任务
     *
     * @param args
     */
    void startSplitData(String[] args) throws Exception;

    /**
     * 开始执行数据归并任务
     *
     * @param args
     */
    void startConvergeData(String[] args) throws Exception;
}
