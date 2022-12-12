package com.wk.data.spark.service.mdm.executor;

/**
 * @Author: smash_hq
 * @Date: 2021/12/14 9:56
 * @Description: 临时表实现方案
 * @Version v1.0
 */

public interface MdmTempService {
    /**
     * 通过临时表实现
     *
     * @param args
     * @throws Exception
     */
    void mdmTemp(String... args) throws Exception;

}
