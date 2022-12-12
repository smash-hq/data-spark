package com.wk.data.spark.facade.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

/**
 * @Author: smash_hq
 * @Date: 2022/3/3 15:29
 * @Description: 操作符枚举
 * @Version v1.0
 */
@AllArgsConstructor
@NoArgsConstructor
@Getter
public enum OperatorEnum {
    EQL("eq", "=", "等于"),
    LTE("lte", "<=", "小于等于"),
    LT("lt", "<", "小于"),
    GTE("gte", ">=", "大于等于"),
    GT("gt", ">", "大于"),
    LIKE("like", "like", "相似"),
    IN("in", "in", "包含"),
    NE("ne", "!=", "不等于");

    private String code;
    private String symbol;
    private String cnName;

    public static String getSymbolByCode(String code) {
        OperatorEnum[] operatorEnum = OperatorEnum.values();
        for (OperatorEnum operator : operatorEnum) {
            if (operator.code.equals(code)) {
                return operator.getSymbol();
            }
        }
        return null;
    }
}
