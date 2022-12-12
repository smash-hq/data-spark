package com.wk.data.spark.infrastructure.master.database.po;

import lombok.*;

import java.io.Serializable;
import java.util.List;

/**
 * @Author: smash_hq
 * @Date: 2021/12/3 11:04
 * @Description: 主数据管理配置信息
 * @Version v1.0
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class MdmCfgDO implements Serializable {
    /**
     * 主键
     */
    private String id;

    /**
     * 主数据名称
     */
    private String mdmName;

    /**
     * 主数据所属项目id
     */
    private String projectId;

    /**
     * 主数据表信息
     */
    private TableDO masterDataTable;

    /**
     * cron表达式
     */
    private String cron;

    /**
     * 更新方式，1 全量，2 增量
     */
    private Integer updateMode;

    /**
     * 描述
     */
    private String description;

    /**
     * 数据源关联信息
     */
    private JoinRelationDO joinRelation;

    /**
     * 集成规则
     */
    private List<SourceRelationDO> sourceRelation;
}
