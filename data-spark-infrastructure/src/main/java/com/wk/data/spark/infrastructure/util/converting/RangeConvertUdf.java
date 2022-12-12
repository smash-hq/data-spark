package com.wk.data.spark.infrastructure.util.converting;

import com.alibaba.fastjson.JSONObject;
import com.wk.data.etl.facade.govern.dto.convert.ConditionDTO;
import org.apache.spark.sql.api.java.UDF3;

import java.util.List;

/**
 * @Created: smash_hq at 17:46 2022/7/27
 * @Description:
 */

public class RangeConvertUdf<T> implements UDF3<T, String, String, String> {


    @Override
    public String call(T t, String rule, String otherValue) throws Exception {
        if (t == null) {
            return otherValue;
        }
        String value = t.toString();
        List<ConditionDTO> list = JSONObject.parseArray(rule, ConditionDTO.class);
        for (int i = 0; i < list.size(); i++) {
            ConditionDTO condition = list.get(i);
            String source = condition.getSource();
            String[] strings = source.split(";");
            for (String string : strings) {
                if (string.equals(value)) {
                    return condition.getTarget();
                }
            }
        }
        return otherValue;
    }
}
