package com.wk.data.spark.service.govern.executor;

/**
 * @program: data-spark-job
 * @description: ETL转换抽象类（T）
 * @author: gwl
 * @create: 2022-08-05 17:56
 **/
public abstract class Transform {

    /**
     * 执行数据转换业务逻辑接口
     * @throws Exception
     */
    public abstract void execute() throws Exception;

}
