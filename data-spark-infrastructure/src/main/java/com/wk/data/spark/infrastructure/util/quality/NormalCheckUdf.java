package com.wk.data.spark.infrastructure.util.quality;

import com.alibaba.fastjson.JSON;
import com.wk.data.etl.facade.quality.dto.NormalCfgDTO;
import com.wk.data.etl.facade.standard.dto.RangeDTO;
import org.apache.spark.sql.api.java.UDF4;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.wk.data.spark.infrastructure.util.General.*;

/**
 * @Author: smash_hq
 * @Date: 2021/12/30 15:25
 * @Description: 常规分析
 * @Version v1.0
 */

public class NormalCheckUdf<T> implements UDF4<T, Long, String, String, String> {

    @Override
    public String call(T value, Long rank, String code, String json) throws Exception {
        List<CaseOpera> list = new ArrayList<>();
        NormalCfgDTO cfg = JSON.parseObject(json, NormalCfgDTO.class);
        // 1 完整性
        boolean integrality = integrality(value, cfg.getIntegrality());
        if (!integrality) {
            list.add(CaseOpera.builder().rowNumberRank(rank).code(code).judge(true).type(INTEGRALITY).build());
        }
        // 2 唯一性 无法使用该自定义函数实现
        // 3 合法性（字符串）
        Integer minString = cfg.getMinString();
        Integer maxString = cfg.getMaxString();
        boolean exactString = exactString(value, minString, maxString);
        if (!exactString) {
            list.add(CaseOpera.builder().rowNumberRank(rank).code(code).judge(true).type(LEGALITY).build());
        }
        // 4 准确性（数值）
        Double minValue = cfg.getMinValue();
        Double maxValue = cfg.getMaxValue();
        boolean exactValue = exactValue(value, minValue, maxValue);
        if (!exactValue) {
            list.add(CaseOpera.builder().rowNumberRank(rank).code(code).judge(true).type(ACCURACY_VALUE).build());
        }
        // 5 准确性（字符串）
        boolean regular = regularRule(value, cfg.getCheckRule());
        if (!regular) {
            list.add(CaseOpera.builder().rowNumberRank(rank).code(code).judge(true).type(ACCURACY_VARCHAR).build());
        }
        // 6 值域
        boolean range = rangeCheck(value, cfg.getRange());
        if (!range) {
            list.add(CaseOpera.builder().rowNumberRank(rank).code(code).judge(true).type(RANGE).build());
        }
        boolean blank = blank(value, cfg.getBlank());
        if (!blank) {
            list.add(CaseOpera.builder().rowNumberRank(rank).code(code).judge(true).type(BLANK).build());
        }
        if (list.isEmpty()) {
            return null;
        }
        return JSON.toJSONString(list);
    }

    private boolean rangeCheck(T value, List<RangeDTO> range) {
        List<String> r = range.stream().filter(it -> !isBlank(it.getRangeCode())
                || !isBlank(it.getRangeName())).map(RangeDTO::getRangeCode).collect(Collectors.toList());
        if (r.isEmpty()) {
            return true;
        }
        if (value == null) {
            return false;
        }
        return r.contains(String.valueOf(value));
    }

    private boolean regularRule(T value, String checkRule) {
        if (checkRule == null || checkRule.length() == 0) {
            return true;
        }
        if (value == null) {
            return false;
        }
        return value.toString().matches(checkRule);
    }

    public boolean isBlank(CharSequence cs) {
        int strLen = cs.length();
        if (strLen == 0) {
            return true;
        }
        for (int i = 0; i < strLen; i++) {
            if (!Character.isWhitespace(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private boolean exactValue(T t, Double minValue, Double maxValue) {
        if ((maxValue == null || maxValue == 0) && (minValue == null || minValue == 0)) {
            return true;
        }
        if (maxValue == null && minValue == null) {
            return true;
        }
        if (t == null) {
            return false;
        }
        double v;
        try {
            v = Double.parseDouble(String.valueOf(t));
        } catch (NumberFormatException ex) {
            return false;
        }
        if (minValue != null && maxValue != null) {
            return v >= minValue && v <= maxValue;
        } else if (minValue != null) {
            return v >= minValue;
        } else {
            return v <= maxValue;
        }
    }


    public boolean integrality(T t, Boolean bool) {
        if (bool == null || !bool) {
            return true;
        }
        return !isEmpty(t);
    }

    private boolean isEmpty(Object object) {
        if (object == null) {
            return true;
        }
        if (object instanceof CharSequence) {
            return ((CharSequence) object).length() == 0;
        }
        if (object.getClass().isArray()) {
            return Array.getLength(object) == 0;
        }
        if (object instanceof Collection<?>) {
            return ((Collection<?>) object).isEmpty();
        }
        if (object instanceof Map<?, ?>) {
            return ((Map<?, ?>) object).isEmpty();
        }
        return false;
    }

    private boolean exactString(T t, Integer minString, Integer maxString) {
        if ((minString == null || minString == 0) && (maxString == null || maxString == 0)) {
            return true;
        }
        if (minString == null && maxString == null) {
            return true;
        }
        if (t == null) {
            return false;
        }
        String value = t.toString();
        int length = value.length();
        if (maxString != null && minString != null) {
            return maxString >= length && minString <= length;
        } else if (minString != null) {
            return minString <= length;
        } else {
            return maxString >= length;
        }
    }

    private boolean blank(T t, Boolean bool) {
        if (bool == null || !bool || t == null) {
            return true;
        }
        String str = t.toString();
        return !str.contains(" ");
    }

}
