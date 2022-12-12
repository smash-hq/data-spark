package com.wk.data.spark.infrastructure.util.enums;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: smash_hq
 * @Date: 2021/11/29 17:14
 * @Description: 数字中文枚举
 * @Version v1.0
 */
@Getter
@NoArgsConstructor
@AllArgsConstructor
public enum ArabicEnum {
    MINUS(-1, "负", "负", "佰", "负"),
    ZERO(0, "零", "零", "", ""),
    ONE(1, "壹", "一", "拾", "十"),
    TWO(2, "贰", "二", "佰", "百"),
    THREE(3, "叁", "三", "仟", "千"),
    FOUR(4, "肆", "四", "万", "万"),
    FIVE(5, "伍", "五", "拾", "十"),
    SIX(6, "陆", "六", "佰", "百"),
    SEVEN(7, "柒", "七", "仟", "千"),
    EIGHT(8, "捌", "八", "亿", "亿"),
    NINE(9, "玖", "九", "拾", "十"),
    TEN(10, "拾", "十", "佰", "百"),
    ELEVEN(11, "拾壹", "十一", "仟", "千"),
    TWELVE(12, "拾贰", "十二", "兆", "兆"),
    THIRTEEN(13, "拾叁", "十三", "拾", "十"),
    FOURTEEN(14, "拾肆", "十四", "佰", "百"),
    FIFTEEN(15, "拾伍", "十五", "仟", "千"),
    SIXTEEN(16, "拾陆", "十六", "京", "京"),
    SEVENTEEN(17, "拾柒", "十七", "拾", "十"),
    EIGHTEEN(18, "拾捌", "十八", "佰", "百"),
    NINETEEN(19, "拾玖", "十九", "仟", "千"),
    TWENTY(20, "贰拾", "二十", "垓", "垓");

    private Integer arabic;
    private String cnName;
    private String cnCode;
    private String units;
    private String octal;

    public static List<String> getCnNames() {
        ArabicEnum[] arabicEnums = ArabicEnum.values();
        List<String> list = new ArrayList<>();
        for (ArabicEnum arabicEnum : arabicEnums) {
            list.add(arabicEnum.getCnName());
        }
        return list;
    }

    public static List<Integer> getArabics() {
        ArabicEnum[] arabicEnums = ArabicEnum.values();
        List<Integer> list = new ArrayList<>();
        for (ArabicEnum arabicEnum : arabicEnums) {
            list.add(arabicEnum.getArabic());
        }
        return list;
    }

    public static List<String> getCnCodes() {
        ArabicEnum[] arabicEnums = ArabicEnum.values();
        List<String> list = new ArrayList<>();
        for (ArabicEnum arabicEnum : arabicEnums) {
            list.add(arabicEnum.getCnCode());
        }
        return list;
    }

    public static String getCnNameByArabic(Integer arabic) {
        String result = null;
        for (ArabicEnum arabicEnum : ArabicEnum.values()) {
            if (arabicEnum.getArabic().equals(arabic)) {
                result = arabicEnum.getCnName();
                break;
            }
        }
        return result;
    }

    public static String getCnCodeByArabic(Integer arabic) {
        String result = null;
        for (ArabicEnum arabicEnum : ArabicEnum.values()) {
            if (arabicEnum.getArabic().equals(arabic)) {
                result = arabicEnum.getCnCode();
                break;
            }
        }
        return result;
    }

    public static Integer getArabicByCnName(String cnName) {
        Integer result = null;
        for (ArabicEnum arabicEnum : ArabicEnum.values()) {
            if (arabicEnum.getCnName().equals(cnName)) {
                result = arabicEnum.getArabic();
                break;
            }
        }
        return result;
    }

    public static Integer getArabicByCnCode(String cnCode) {
        Integer result = null;
        for (ArabicEnum arabicEnum : ArabicEnum.values()) {
            if (arabicEnum.getCnCode().equals(cnCode)) {
                result = arabicEnum.getArabic();
                break;
            }
        }
        return result;
    }

    public static String getUnitByArabic(Integer arabic) {
        String result = null;
        for (ArabicEnum arabicEnum : ArabicEnum.values()) {
            if (arabicEnum.getArabic().equals(arabic)) {
                result = arabicEnum.getUnits();
                break;
            }
        }
        return result;
    }

    public static String getOctalByArabic(Integer arabic) {
        String result = null;
        for (ArabicEnum arabicEnum : ArabicEnum.values()) {
            if (arabicEnum.getArabic().equals(arabic)) {
                result = arabicEnum.getOctal();
                break;
            }
        }
        return result;
    }

    public static Integer getArabicByUnits(String units) {
        Integer result = null;
        for (ArabicEnum arabicEnum : ArabicEnum.values()) {
            if (arabicEnum.getUnits().equals(units)) {
                result = arabicEnum.getArabic();
                break;
            }
        }
        return result;
    }
}
