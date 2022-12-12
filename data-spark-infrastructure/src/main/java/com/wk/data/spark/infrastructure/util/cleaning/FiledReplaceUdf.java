package com.wk.data.spark.infrastructure.util.cleaning;

import org.apache.spark.sql.api.java.UDF4;

/**
 * @Author: smash_hq
 * @Date: 2021/11/25 16:05
 * @Description: 字段内容替换
 * @Version v1.0
 */

public class FiledReplaceUdf implements UDF4<String, String, String, Integer, String> {

    /**
     * functions.expr("filedReplace(name,'参数','test',0)")
     *
     * @param colName 操作字段
     * @param preStr  被替换字符串
     * @param fuStr   替换的字符串
     * @param i       0 匹配为前置才替换   1 匹配后置才替换   2 任意位置皆替换
     * @return
     */

    @Override
    public String call(String colName, String preStr, String fuStr, Integer i) {
        if (colName == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        String oldCol = colName;
        int preSize = preStr.length();
        int oldSize = oldCol.length();
        switch (i) {
            case 0: {
                boolean flag = oldCol.startsWith(preStr);
                if (flag) {
                    String newCol = oldCol.substring(preSize);
                    return sb.append(fuStr).append(newCol).toString();
                }
                return oldCol;
            }
            case 1: {
                boolean flag = oldCol.endsWith(preStr);
                int diff = oldSize - preSize - 1;
                if (flag) {
                    String newCol = oldCol.substring(0, diff);
                    return sb.append(newCol).append(fuStr).toString();
                }
                return oldCol;
            }
            case 2: {
                boolean flag = oldCol.contains(preStr);
                if (flag) {
                    return oldCol.replace(preStr, fuStr);
                }
                return oldCol;
            }
            default:
                return oldCol;
        }
    }
}
