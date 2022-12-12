package com.wk.data.spark.infrastructure.util.quality;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @Author: smash_hq
 * @Date: 2022/1/6 15:01
 * @Description: 运算使用
 * @Version v1.0
 */
@Builder
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Setter
public class CaseOpera {

    private Long rowNumberRank;
    private String code;
    private String type;
    private Boolean judge;
}
