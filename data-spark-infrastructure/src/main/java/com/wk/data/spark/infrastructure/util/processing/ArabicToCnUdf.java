package com.wk.data.spark.infrastructure.util.processing;

import com.wk.data.spark.infrastructure.util.enums.ArabicEnum;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF2;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * @Author: smash_hq
 * @Date: 2021/11/29 16:52
 * @Description: 阿拉伯数字替换为中文数字
 * @Version v1.0
 */

public class ArabicToCnUdf implements UDF2<String, Integer, String> {
    public static final String DOT = ".";
    public static final String MINUS = "-";


    /**
     * functions.expr("arabicToCnUDF(run_times,1)")
     *
     * @param value
     * @param mode  123->   1 一百二十三 2 壹佰贰拾叁 3 一二三 4 壹贰叁
     * @return
     * @throws Exception
     */
    @Override
    public String call(String value, Integer mode) {
        if (!StringUtils.isNumeric(value) || judge(value)) {
            return value;
        }
        BigDecimal bigDecimal = new BigDecimal(value);
        switch (mode) {
            case 1:
                return excution3(bigDecimal);
            case 2:
                return excution1(bigDecimal);
            case 3:
                return excution4(bigDecimal);
            case 4:
                return excution2(bigDecimal);
            default:
                return null;
        }
    }

    private static String excution4(BigDecimal bigDecimal) {
        StringBuilder sb = new StringBuilder();
        String num = bigDecimal.toString();
        // 小数
        int index = num.indexOf(DOT);
        String decimal = num.substring(index + 1);
        int scale = bigDecimal.scale();

        String str = index == -1 ? num : num.substring(0, index);
        for (int i = 0; i < str.length(); i++) {
            String s = MINUS.equals(String.valueOf(str.charAt(i))) ? "-1" : String.valueOf(str.charAt(i));
            int indexOf = Integer.parseInt(s);
            String cnName = ArabicEnum.getCnCodeByArabic(indexOf);
            sb.append(cnName == null ? "" : cnName);
        }
        if (index > 0) {
            getDotToCnCode(sb, decimal, scale);
        }
        return sb.toString();
    }

    private static String excution3(BigDecimal bigDecimal) {
        StringBuilder sb = new StringBuilder();
        boolean flag = bigDecimal.compareTo(new BigDecimal(0)) < 0;
        String num = bigDecimal.abs().toString();
        BigInteger integer = bigDecimal.abs().toBigInteger();
        String str = String.valueOf(integer);
        int numOfConcecutiveZero = 0;
        int length = str.length();
        for (int i = 0; i < length; i++) {
            int charToInt = str.charAt(i) - 48;
            int correspondingIndex = length - 1 - i;
            if (str.charAt(i) == '0') {
                numOfConcecutiveZero++;
                if (correspondingIndex == 12 || correspondingIndex == 8 || correspondingIndex == 4) {
                    if (numOfConcecutiveZero == 4) {
                        numOfConcecutiveZero = 0;
                        continue;
                    }
                    sb.append(ArabicEnum.getOctalByArabic(correspondingIndex));
                    numOfConcecutiveZero = 0;
                }
                if (correspondingIndex == 0) {
                    sb.append(ArabicEnum.getOctalByArabic(correspondingIndex));
                }
            } else if (numOfConcecutiveZero == 0) {
                sb.append(ArabicEnum.getCnCodeByArabic(charToInt));
                sb.append(ArabicEnum.getOctalByArabic(correspondingIndex));
            } else {
                numOfConcecutiveZero = 0;
                sb.append(ArabicEnum.getCnCodeByArabic(0));
                sb.append(ArabicEnum.getCnCodeByArabic(charToInt));
                sb.append(ArabicEnum.getOctalByArabic(correspondingIndex));
            }
        }

        // 小数
        int index = num.indexOf(DOT);
        String decimal = num.substring(index + 1);
        int scale = bigDecimal.scale();
        if (index > 0) {
            getDotToCnCode(sb, decimal, scale);
        }
        if (flag) {
            return ArabicEnum.getCnCodeByArabic(-1) + sb;
        }
        return sb.toString();
    }

    private static String excution1(BigDecimal bigDecimal) {
        StringBuilder sb = new StringBuilder();
        boolean flag = bigDecimal.compareTo(new BigDecimal(0)) < 0;
        String num = bigDecimal.abs().toString();
        BigInteger longVal = bigDecimal.abs().toBigInteger();
        String str = String.valueOf(longVal);
        int numOfConcecutiveZero = 0;
        int length = str.length();
        for (int i = 0; i < length; i++) {
            int charToInt = str.charAt(i) - 48;
            int correspondingIndex = length - 1 - i;
            if (str.charAt(i) == '0') {
                numOfConcecutiveZero++;
                if (correspondingIndex == 8 || correspondingIndex == 4) {
                    if (numOfConcecutiveZero == 4) {
                        numOfConcecutiveZero = 0;
                        continue;
                    }
                    sb.append(ArabicEnum.getUnitByArabic(correspondingIndex));
                    numOfConcecutiveZero = 0;
                }
                if (correspondingIndex == 0) {
                    sb.append(ArabicEnum.getUnitByArabic(correspondingIndex));
                }
            } else if (numOfConcecutiveZero == 0) {
                sb.append(ArabicEnum.getCnNameByArabic(charToInt));
                sb.append(ArabicEnum.getUnitByArabic(correspondingIndex));
            } else {
                numOfConcecutiveZero = 0;
                sb.append(ArabicEnum.getCnNameByArabic(0));
                sb.append(ArabicEnum.getCnNameByArabic(charToInt));
                sb.append(ArabicEnum.getUnitByArabic(correspondingIndex));
            }
        }

        // 小数
        int index = num.indexOf(DOT);
        String decimal = num.substring(index + 1);
        int scale = bigDecimal.scale();
        if (index > 0) {
            getDotToCnName(sb, decimal, scale);
        }
        if (flag) {
            return ArabicEnum.getCnNameByArabic(-1) + sb;
        }
        return sb.toString();
    }

    private static String excution2(BigDecimal bigDecimal) {
        StringBuilder sb = new StringBuilder();
        String num = bigDecimal.toString();
        // 小数
        int index = num.indexOf(DOT);
        String decimal = num.substring(index + 1);
        int scale = bigDecimal.scale();

        String str = index == -1 ? num : num.substring(0, index);
        for (int i = 0; i < str.length(); i++) {
            String s = MINUS.equals(String.valueOf(str.charAt(i))) ? "-1" : String.valueOf(str.charAt(i));
            int indexOf = Integer.parseInt(s);
            String cnName = ArabicEnum.getCnNameByArabic(indexOf);
            sb.append(cnName == null ? "" : cnName);
        }
        getDotToCnName(sb, decimal, scale);
        return sb.toString();
    }

    public static void main(String[] args) {
        String num = "-5481231232023234548.2";
        System.out.println(excution1(new BigDecimal(num)));
    }

    public static boolean judge(String str) {
        boolean flag = StringUtils.isBlank(str);
        try {
            BigDecimal bigDecimal = new BigDecimal(str);
        } catch (NumberFormatException e) {
            flag = true;
        }
        return flag;
    }


    private static void getDotToCnName(StringBuilder sb, String decimal, int scale) {
        if (scale != 0) {
            sb.append("点");
            for (int i = 0; i < decimal.length(); i++) {
                char c = decimal.charAt(i);
                String s = String.valueOf(c);
                sb.append(ArabicEnum.getCnNameByArabic(Integer.valueOf(s)));
            }
        }
    }

    private static void getDotToCnCode(StringBuilder sb, String decimal, int scale) {
        if (scale != 0) {
            sb.append("点");
            for (int i = 0; i < decimal.length(); i++) {
                char c = decimal.charAt(i);
                String s = String.valueOf(c);
                sb.append(ArabicEnum.getCnCodeByArabic(Integer.valueOf(s)));
            }
        }
    }
}
