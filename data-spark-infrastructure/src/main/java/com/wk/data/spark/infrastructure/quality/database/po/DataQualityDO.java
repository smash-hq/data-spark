/*
package com.wk.data.spark.infrastructure.quality.database.po;

import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.*;
import org.springframework.format.annotation.DateTimeFormat;

import java.io.Serializable;
import java.util.Date;

*/
/**
 * @Author: smash_hq
 * @Date: 2021/8/27 10:40
 * @Description: 质量规则
 * @Version v1.0
 *//*

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@Builder
@ToString
//@Document(collection = "quality_data_cfg")
public class DataQualityDO implements Serializable {
    //    private String id;
//    private String databaseCode;
//    private String tableCode;
//    private String ruleSerial;
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    @DateTimeFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date updateTime;
    */
/**
     * 报告详情，spark 将最新一次写入数据库
     *//*

    private String reportId;
    private Integer problemTotal;
    private Integer detectionTotal;
}
*/
