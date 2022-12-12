package com.wk.data.spark.infrastructure.util.cleaning;

import org.apache.spark.sql.api.java.UDF4;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Author: smash_hq
 * @Date: 2021/11/25 17:38
 * @Description: 删除字符串前后字符
 * @Version v1.0
 */

public class FiledDeleteByCharUdf implements UDF4<String, String, Integer, Integer, String> {
    /**
     * functions.expr("filedDeleteByChar(name,'0',5,1)")
     *
     * @param value   操作字段
     * @param findStr 搜索字符串
     * @param count   删除的字符串数量
     * @param pos     0 从前 1 从后
     * @return
     */
    @Override
    public String call(String value, String findStr, Integer count, Integer pos) {
        if (value == null) {
            return null;
        }
        String col = value;
        boolean flag = col.contains(findStr);
        if (flag) {
            switch (pos) {
                case 0:
                    return preDelete(col, findStr, count);
                case 1:
                    return backDelete(col, findStr, count);
                default:
                    break;
            }
        }
        return col;
    }


    public static void main(String[] args) {
        String col = "aKeyabKeycdefgKeyhijklmnKeyopq5555Key";
        // 查找匹配字符
        String findStr = "Key";
        // 删除个数 4 个
        int delIndex = 2;

        System.out.println(backDelete(col, findStr, delIndex));
        System.out.println(preDelete(col, findStr, delIndex));
    }

    private static String backDelete(String col, String findStr, Integer delIndex) {
        StringBuilder sb = new StringBuilder();
        int colSize = col.length();
        int findSize = findStr.length();
        int symbol = 0;
        List<Integer> list = exec(col, findStr);
        for (int j = 0; j < list.size(); j++) {
            int i = j == list.size() - 1 ? colSize : list.get(j + 1);
            String tmp = col.substring(symbol, i);
            symbol = i;
            String firstStr = tmp.substring(0, tmp.indexOf(findStr) + findSize);
            String secondStr = tmp.substring(tmp.indexOf(findStr) + findSize);
            int endIndex = Math.min(secondStr.length(), delIndex);
            String odd = secondStr.substring(endIndex);
            sb.append(firstStr).append(odd);
        }
        return sb.toString();
    }

    private static String preDelete(String col, String findStr, int delIndex) {
        StringBuilder sb = new StringBuilder();
        int colSize = col.length();
        int findSize = findStr.length();
        int symbol = 0;
        List<Integer> list = exec(col, findStr).stream().map(it -> it + findSize).collect(Collectors.toList());
        for (int j = 0; j < list.size(); j++) {
            if (j < list.size() - 1) {
                int i = list.get(j);
                String tmp = col.substring(symbol, i);
                symbol = i;
                int tmpSize = tmp.length();
                int endIndex = Math.max(tmpSize - findSize - delIndex, 0);
                String str = tmp.substring(0, endIndex);
                sb.append(str).append(findStr);
            } else if (j == list.size() - 1) {
                String tmp = col.substring(symbol, colSize);
                int tmpSize = tmp.length();
                String endStr = tmp.substring(tmp.indexOf(findStr) + findSize, tmpSize);
                int endIndex = Math.max(tmpSize - endStr.length() - findSize - delIndex, 0);
                String str = tmp.substring(0, endIndex);
                sb.append(str).append(findStr).append(endStr);
            }
        }
        return sb.toString();
    }

    private static List<Integer> exec(String col, String findStr) {
        List<Integer> list = new ArrayList<>();
        int j = Integer.MIN_VALUE;
        for (int i = 0; i < col.length(); i++) {
            int k = col.indexOf(findStr, i);
            if (j != k && k != -1) {
                list.add(k);
                j = k;
            }
        }
        return list;
    }
}
