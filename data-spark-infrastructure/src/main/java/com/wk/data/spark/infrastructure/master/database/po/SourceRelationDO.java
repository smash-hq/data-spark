package com.wk.data.spark.infrastructure.master.database.po;

import lombok.*;

import java.io.Serializable;

/**
 * @Author: smash_hq
 * @Date: 2021/12/2 17:27
 * @Description: 字段配置信息
 * @Version v1.0
 */

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class SourceRelationDO implements Serializable {
    /**
     * 主数据表字段代码
     */
    private String masterCode;
    /**
     * 主数据表字段名称
     */
    private String masterName;
    /**
     * 主数据表字段类型
     */
    private String masterType;

    /**
     * 第一来源
     */
    private String firstTable;
    private String firstFiled;

    /**
     * 第二来源
     */
    private String secondTable;
    private String secondFiled;

    /**
     * 第三来源
     */
    private String thirdTable;
    private String thirdFiled;

}
