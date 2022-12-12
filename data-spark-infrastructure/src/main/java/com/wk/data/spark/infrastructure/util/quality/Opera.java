package com.wk.data.spark.infrastructure.util.quality;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

import java.util.Map;

/**
 * @Author: smash_hq
 * @Date: 2022/1/6 18:11
 * @Description: 详情
 * @Version v1.0
 */
@Builder
@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Setter
public class Opera {
    private Long rowNumberRank;
    private Map<String, Map<String, Boolean>> lightAndSearch;
}
