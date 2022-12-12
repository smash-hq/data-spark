package com.wk.data.spark.infrastructure.master.database.po;

import lombok.*;

import java.io.Serializable;

/**
 * @Author: smash_hq
 * @Date: 2021/12/2 14:57
 * @Description: 表基本信息
 * @Version v1.0
 */

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class TableDO implements Serializable {
    /**
     * 数据层
     */
    private String layerCode;

    /**
     * 数据库
     */
    private String database;

    /**
     * 表代码
     */
    private String tableCode;

    /**
     * 主表关联字段
     */
    private String leftFiled;

    /**
     * 副表关联字段
     */
    private String rightFiled;
}
