package com.wk.data.spark.infrastructure.util.processing;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF1;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.regex.Pattern;

/**
 * @Author: smash_hq
 * @Date: 2021/11/29 16:15
 * @Description: 身份证标准化（15>18）
 * @Version v1.0
 */

public class GenerateIdCardUdf implements UDF1<String, String> {
    private static final int CHINA_ID_MIN_LENGTH = 15;
    private static final int CHINA_ID_MAX_LENGTH = 18;
    private static final int[] POWER = {7, 9, 10, 5, 8, 4, 2, 1, 6, 3, 7, 9, 10, 5, 8, 4, 2};
    private static final String NUMBERS = "\\d+";

    /**
     * 只支持15位转18位，自行处理不满足长度情况
     *
     * @param oldId
     * @return
     * @throws Exception
     */
    @Override
    public String call(String oldId) {
        if (StringUtils.isBlank(oldId)) {
            return oldId;
        }
        if (oldId.length() == CHINA_ID_MAX_LENGTH) {
            return oldId;
        }
        return convert15To18(oldId);
    }

    public String convert15To18(String idCard) {
        StringBuilder idCard18 = new StringBuilder();
        if (idCard.length() != CHINA_ID_MIN_LENGTH) {
            return null;
        }
        Pattern num = Pattern.compile(NUMBERS);
        if (num.matcher(idCard).matches()) {
            // 获取出生年月日
            String birthday = idCard.substring(6, 12);
            LocalDate birthDate = LocalDate.parse(birthday, DateTimeFormatter.ofPattern("yyMMdd"));
            // 获取出生年(完全表现形式,如：2010)
            int sYear = birthDate.getYear();
            int maxYear = 2000;
            if (sYear > maxYear) {
                // 2000年之后不存在15位身份证号，此处用于修复此问题的判断
                sYear -= 100;
            }
            idCard18.append(idCard, 0, 6).append(sYear).append(idCard.substring(8));
            // 获取校验位
            char sVal = getCheckCode18(getPowerSum(idCard18.toString().toCharArray()));
            idCard18.append(sVal);
        } else {
            return null;
        }
        return idCard18.toString();
    }

    private static int getPowerSum(char[] iArr) {
        int iSum = 0;
        if (POWER.length == iArr.length) {
            for (int i = 0; i < iArr.length; i++) {
                iSum += Integer.parseInt(String.valueOf(iArr[i])) * POWER[i];
            }
        }
        return iSum;
    }


    private char getCheckCode18(int iSum) {
        switch (iSum % 11) {
            case 10:
                return '2';
            case 9:
                return '3';
            case 8:
                return '4';
            case 7:
                return '5';
            case 6:
                return '6';
            case 5:
                return '7';
            case 4:
                return '8';
            case 3:
                return '9';
            case 2:
                return 'X';
            case 1:
                return '0';
            case 0:
                return '1';
            default:
                return ' ';
        }
    }

}
