package com.wk.data.spark.infrastructure.master.database.po;

import lombok.*;

import java.io.Serializable;
import java.util.List;

/**
 * @Author: smash_hq
 * @Date: 2021/12/2 14:52
 * @Description: 关联表列表
 * @Version v1.0
 */

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
public class JoinRelationDO implements Serializable {
    /**
     * 主表信息
     */
    private TableDO leftTable;

    /**
     * 副表信息
     */
    private List<TableDO> rightTables;

}
