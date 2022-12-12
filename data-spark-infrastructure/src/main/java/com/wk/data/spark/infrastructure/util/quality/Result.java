package com.wk.data.spark.infrastructure.util.quality;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * @Author: smash_hq
 * @Date: 2022/1/11 10:19
 * @Description: 结果信息
 * @Version v1.0
 */
@Builder
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Setter
public class Result {
    private String type;
    private Boolean judge;
}
