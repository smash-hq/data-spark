package com.wk.data.spark.infrastructure.util.converting;

import com.alibaba.fastjson.JSONObject;
import com.wk.data.etl.facade.govern.dto.convert.ConditionDTO;
import org.apache.spark.sql.api.java.UDF3;

import java.util.List;

/**
 * @Created: smash_hq at 16:56 2022/7/27
 * @Description: 值域转换
 */

public class ValueConvertUdf<T> implements UDF3<T, String, String, String> {


    @Override
    public String call(T t, String rule, String otherValue) throws Exception {
        if (t == null) {
            return otherValue;
        }
        long value = Long.parseLong(t.toString());
        List<ConditionDTO> list = JSONObject.parseArray(rule, ConditionDTO.class);
        for (int i = 0; i < list.size(); i++) {
            ConditionDTO condition = list.get(i);
            String source = condition.getSource();
            String[] strings = source.split(",");
            long left = Long.parseLong(strings[0]);
            long right = Long.parseLong(strings[1]);
            if (i < list.size() - 1) {
                if (value >= left && value < right) {
                    return condition.getTarget();
                }
            } else {
                if (value >= left && value <= right) {
                    return condition.getTarget();
                }
            }
        }
        return otherValue;
    }
}
