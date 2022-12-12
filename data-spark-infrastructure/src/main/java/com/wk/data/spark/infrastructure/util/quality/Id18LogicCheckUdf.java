package com.wk.data.spark.infrastructure.util.quality;

import com.alibaba.fastjson.JSON;
import org.apache.spark.sql.api.java.UDF3;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: smash_hq
 * @Date: 2022/1/5 10:20
 * @Description: 身份证18位合法性检测
 * @Version v1.0
 */

public class Id18LogicCheckUdf<T> implements UDF3<T, Long, String, String> {
    @Override
    public String call(T t, Long rank, String code) throws Exception {
        List<CaseOpera> list = new ArrayList<>();
        if (t == null) {
            list.add(CaseOpera.builder().rowNumberRank(rank).code(code).judge(true).type("idCard").build());
            return JSON.toJSONString(list);
        }
        String patten = "^[1-9]\\d{5}(18|19|([23]\\d))\\d{2}((0[1-9])|(10|11|12))(([0-2][1-9])|10|20|30|31)\\d{3}[0-9Xx]$";
        String id = String.valueOf(t);
        boolean result = id.matches(patten);
        if (!result) {
            list.add(CaseOpera.builder().rowNumberRank(rank).code(code).judge(true).type("idCard").build());
        }
        if (list.isEmpty()) {
            return null;
        }
        return JSON.toJSONString(list);
    }
}
