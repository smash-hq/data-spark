package com.wk.data.spark.infrastructure.util.cleaning;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF3;

/**
 * @Created: smash_hq at 14:43 2022/9/27
 * @Description: 特殊赋值函数
 */

public class FiledAssignValueUdf implements UDF3<String, String, Integer, String> {
    @Override
    public String call(String code, String sourceCode, Integer assign) throws Exception {
        switch (assign) {
            case 1: // anyway
                code = sourceCode;
                break;
            case 2: // is blank
                if (code == null || StringUtils.isBlank(code)) {
                    code = sourceCode;
                }
                break;
            case 3: // is null
                if (code == null) {
                    code = sourceCode;
                }
                break;
            default:
                break;
        }
        return code;
    }
}
