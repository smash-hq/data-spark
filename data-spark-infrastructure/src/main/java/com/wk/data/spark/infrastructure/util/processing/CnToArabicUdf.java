package com.wk.data.spark.infrastructure.util.processing;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF2;

import java.math.BigDecimal;

import static com.wk.data.spark.infrastructure.util.enums.ArabicEnum.*;

/**
 * @Author: smash_hq
 * @Date: 2021/11/29 16:52
 * @Description: 阿拉伯数字替换为中文数字
 * @Version v1.0
 */

public class CnToArabicUdf implements UDF2<String, Integer, String> {
    public static final String DOST = "点";
    public static final String DOT = ".";
    public static final String MINUS = "负";

    /**
     * functions.expr("arabicToCnUDF(name,1)")
     *
     * @param str
     * @param i   1 一百二十三->123 2 壹佰贰拾叁->123  3 一二三->123 4 壹贰叁->123
     * @return
     * @throws Exception
     */
    @Override
    public String call(String str, Integer i) {
        if (judge(str, i)) {
            return null;
        }
        switch (i) {
            case 1:
                return excution3(str);
            case 2:
                return excution1(str);
            case 3:
                return excution4(str);
            case 4:
                return excution2(str);
            default:
                break;
        }
        return null;
    }

    private static String excution4(String str) {
        StringBuilder sb = new StringBuilder();
        int index = str.indexOf(DOST);
        String integer = str.substring(0, index == -1 ? str.length() : index);

        for (int i = 0; i < integer.length(); i++) {
            char c = integer.charAt(i);
            String s = String.valueOf(c);
            int tmpInt = getArabicByCnCode(s);
            sb.append(tmpInt);
        }
        if (index > -1) {
            String decimalStr = str.substring(index + 1);
            sb.append(DOT).append(getDecimalByCnCode(decimalStr));
        }
        return sb.toString();
    }

    private static String excution3(String str) {
        StringBuilder sb = new StringBuilder();
        long res = 0;
        int minus = str.indexOf(MINUS);
        int index = str.indexOf(DOST);
        if (minus > -1) {
            str = str.substring(minus + 1);
        }
        String intStr = str.substring(0, index == -1 ? str.length() : index);
        // 亿
        int billions = intStr.indexOf(getOctalByArabic(8));
        if (billions > 0) {
            String tmp = intStr.substring(0, billions);
            int thousand = tmp.indexOf(getOctalByArabic(3));
            if (thousand > 0) {
                char cnName = tmp.charAt(thousand - 1);
                String cn = String.valueOf(cnName);
                int tmpInt = getArabicByCnCode(cn);
                res = res + tmpInt * 100000000000L;
            }
            int hundred = tmp.indexOf(getOctalByArabic(2));
            if (hundred > 0) {
                char cnName = tmp.charAt(hundred - 1);
                String cn = String.valueOf(cnName);
                int tmpInt = getArabicByCnCode(cn);
                res = res + tmpInt * 10000000000L;
            }
            int tens = tmp.indexOf(getOctalByArabic(1));
            if (tens > 0) {
                char cnName = tmp.charAt(tens - 1);
                String cn = String.valueOf(cnName);
                int tmpInt = getArabicByCnCode(cn);
                res = res + tmpInt * 1000000000L;
            }
            char cnName = tmp.charAt(tmp.length() - 1);
            String cn = String.valueOf(cnName);
            int tmpInt = getArabicByCnCode(cn);
            res = res + tmpInt * 100000000L;
        }
        // 万
        int tensIndex = intStr.indexOf(getOctalByArabic(4));
        if (tensIndex > 0) {
            String tmp = intStr.substring(billions == -1 ? 0 : billions + 1, tensIndex);
            int thousand = tmp.indexOf(getOctalByArabic(3));
            if (thousand > 0) {
                char cnName = tmp.charAt(thousand - 1);
                String cn = String.valueOf(cnName);
                int tmpInt = getArabicByCnCode(cn);
                res = res + tmpInt * 10000000L;
            }
            int hundred = tmp.indexOf(getOctalByArabic(2));
            if (hundred > 0) {
                char cnName = tmp.charAt(hundred - 1);
                String cn = String.valueOf(cnName);
                int tmpInt = getArabicByCnCode(cn);
                res = res + tmpInt * 1000000L;
            }
            int tens = tmp.indexOf(getOctalByArabic(1));
            if (tens > 0) {
                char cnName = tmp.charAt(tens - 1);
                String cn = String.valueOf(cnName);
                int tmpInt = getArabicByCnCode(cn);
                res = res + tmpInt * 100000L;
            }
            char cnName = tmp.charAt(tmp.length() - 1);
            String cn = String.valueOf(cnName);
            int tmpInt = getArabicByCnCode(cn);
            res = res + tmpInt * 10000L;
        }
        // 个
        String tmp = intStr.substring(tensIndex == -1 ? 0 : tensIndex + 1);
        int thousand = tmp.indexOf(getOctalByArabic(3));
        if (thousand > 0) {
            char cnName = tmp.charAt(thousand - 1);
            String cn = String.valueOf(cnName);
            int tmpInt = getArabicByCnCode(cn);
            res = res + tmpInt * 1000L;
        }
        int hundred = tmp.indexOf(getOctalByArabic(2));
        if (hundred > 0) {
            char cnName = tmp.charAt(hundred - 1);
            String cn = String.valueOf(cnName);
            int tmpInt = getArabicByCnCode(cn);
            res = res + tmpInt * 100L;
        }
        int tens = tmp.indexOf(getOctalByArabic(1));
        if (tens > 0) {
            char cnName = tmp.charAt(tens - 1);
            String cn = String.valueOf(cnName);
            int tmpInt = getArabicByCnCode(cn);
            res = res + tmpInt * 10L;
        }
        char cnName = tmp.charAt(tmp.length() - 1);
        String cn = String.valueOf(cnName);
        int tmpInt = getArabicByCnCode(cn);
        res = res + (long) tmpInt;
        sb.append(res);

        if (index > -1) {
            String decimalStr = str.substring(index + 1);
            sb.append(DOT).append(getDecimalByCnCode(decimalStr));
        }
        return sb.toString();
    }

    private static String excution2(String str) {
        StringBuilder sb = new StringBuilder();
        int index = str.indexOf(DOST);
        String integer = str.substring(0, index == -1 ? str.length() : index);

        for (int i = 0; i < integer.length(); i++) {
            char c = integer.charAt(i);
            String s = String.valueOf(c);
            int tmpInt = getArabicByCnName(s);
            sb.append(tmpInt);
        }
        if (index > -1) {
            String decimalStr = str.substring(index + 1);
            sb.append(DOT).append(getDecimalByCnName(decimalStr));
        }
        return sb.toString();
    }

    private static String excution1(String str) {
        StringBuilder sb = new StringBuilder();
        long res = 0;
        int minus = str.indexOf(MINUS);
        int index = str.indexOf(DOST);
        if (minus > -1) {
            str = str.substring(minus + 1);
        }
        String intStr = str.substring(0, index == -1 ? str.length() : index);
        // 亿
        int billions = intStr.indexOf(getUnitByArabic(8));
        if (billions > 0) {
            String tmp = intStr.substring(0, billions);
            int thousand = tmp.indexOf(getUnitByArabic(3));
            if (thousand > 0) {
                char cnName = tmp.charAt(thousand - 1);
                String cn = String.valueOf(cnName);
                int tmpInt = getArabicByCnName(cn);
                res = res + tmpInt * 100000000000L;
            }
            int hundred = tmp.indexOf(getUnitByArabic(2));
            if (hundred > 0) {
                char cnName = tmp.charAt(hundred - 1);
                String cn = String.valueOf(cnName);
                int tmpInt = getArabicByCnName(cn);
                res = res + tmpInt * 10000000000L;
            }
            int tens = tmp.indexOf(getUnitByArabic(1));
            if (tens > 0) {
                char cnName = tmp.charAt(tens - 1);
                String cn = String.valueOf(cnName);
                int tmpInt = getArabicByCnName(cn);
                res = res + tmpInt * 1000000000L;
            }
            char cnName = tmp.charAt(tmp.length() - 1);
            String cn = String.valueOf(cnName);
            int tmpInt = getArabicByCnName(cn);
            res = res + tmpInt * 100000000L;
        }
        // 万
        int tensIndex = intStr.indexOf(getUnitByArabic(4));
        if (tensIndex > 0) {
            String tmp = intStr.substring(billions == -1 ? 0 : billions + 1, tensIndex);
            int thousand = tmp.indexOf(getUnitByArabic(3));
            if (thousand > 0) {
                char cnName = tmp.charAt(thousand - 1);
                String cn = String.valueOf(cnName);
                int tmpInt = getArabicByCnName(cn);
                res = res + tmpInt * 10000000L;
            }
            int hundred = tmp.indexOf(getUnitByArabic(2));
            if (hundred > 0) {
                char cnName = tmp.charAt(hundred - 1);
                String cn = String.valueOf(cnName);
                int tmpInt = getArabicByCnName(cn);
                res = res + tmpInt * 1000000L;
            }
            int tens = tmp.indexOf(getUnitByArabic(1));
            if (tens > 0) {
                char cnName = tmp.charAt(tens - 1);
                String cn = String.valueOf(cnName);
                int tmpInt = getArabicByCnName(cn);
                res = res + tmpInt * 100000L;
            }
            char cnName = tmp.charAt(tmp.length() - 1);
            String cn = String.valueOf(cnName);
            int tmpInt = getArabicByCnName(cn);
            res = res + tmpInt * 10000L;
        }
        // 个
        String tmp = intStr.substring(tensIndex == -1 ? 0 : tensIndex + 1);
        int thousand = tmp.indexOf(getUnitByArabic(3));
        if (thousand > 0) {
            char cnName = tmp.charAt(thousand - 1);
            String cn = String.valueOf(cnName);
            int tmpInt = getArabicByCnName(cn);
            res = res + tmpInt * 1000L;
        }
        int hundred = tmp.indexOf(getUnitByArabic(2));
        if (hundred > 0) {
            char cnName = tmp.charAt(hundred - 1);
            String cn = String.valueOf(cnName);
            int tmpInt = getArabicByCnName(cn);
            res = res + tmpInt * 100L;
        }
        int tens = tmp.indexOf(getUnitByArabic(1));
        if (tens > 0) {
            char cnName = tmp.charAt(tens - 1);
            String cn = String.valueOf(cnName);
            int tmpInt = getArabicByCnName(cn);
            res = res + tmpInt * 10L;
        }
        char cnName = tmp.charAt(tmp.length() - 1);
        String cn = String.valueOf(cnName);
        int tmpInt = getArabicByCnName(cn);
        res = res + (long) tmpInt;
        sb.append(res);

        if (index > -1) {
            String decimalStr = str.substring(index + 1);
            sb.append(DOT).append(getDecimalByCnName(decimalStr));
        }
        return sb.toString();
    }

    private static String getDecimalByCnName(String decimalStr) {
        StringBuilder decimalBuilder = new StringBuilder();
        int decimalLength = decimalStr.length();
        for (int i = 0; i < decimalLength; i++) {
            char symbol = decimalStr.charAt(i);
            String v = String.valueOf(symbol);
            int value = getArabicByCnName(v);
            decimalBuilder.append(value);
        }
        return decimalBuilder.toString();
    }

    private static String getDecimalByCnCode(String decimalStr) {
        StringBuilder decimalBuilder = new StringBuilder();
        int decimalLength = decimalStr.length();
        for (int i = 0; i < decimalLength; i++) {
            char symbol = decimalStr.charAt(i);
            String v = String.valueOf(symbol);
            int value = getArabicByCnCode(v);
            decimalBuilder.append(value);
        }
        return decimalBuilder.toString();
    }

    private static boolean judge(String str, Integer mode) {
        boolean flag = StringUtils.isBlank(str);
        try {
            BigDecimal bigDecimal = new BigDecimal(str);
        } catch (NumberFormatException e) {
            return true;
        }

        for (char s : str.toCharArray()) {
            String ss = String.valueOf(s);
            boolean flag1 = mode.equals(1) || mode.equals(2);
            boolean flag2 = mode.equals(3) || mode.equals(4);
            boolean flag3 = flag1 && !getCnNames().contains(ss);
            boolean flag4 = flag2 && !getCnCodes().contains(ss);
            if (flag3 || flag4) {
                flag = true;
            }
        }
        return flag;
    }

    public static void main(String[] args) {
        String s1 = "jfd.2sadf";
        System.out.println(judge(s1, 2));
    }

}
