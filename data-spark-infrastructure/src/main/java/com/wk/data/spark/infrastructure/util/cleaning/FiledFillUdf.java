package com.wk.data.spark.infrastructure.util.cleaning;


import org.apache.commons.lang.StringUtils;
import org.apache.spark.sql.api.java.UDF4;

/**
 * @Author: smash_hq
 * @Date: 2021/11/25 15:44
 * @Description: 字段填充
 * @Version v1.0
 */
public class FiledFillUdf implements UDF4<String, String, Integer, Integer, String> {

    /**
     * functions.expr("filedFill(name,'append',1)")
     *
     * @param value  操作字段
     * @param str    填充的字符
     * @param pos    0-添加在开头 1-添加在结尾
     * @param length 填充后的字符串长度
     * @return
     */
    @Override
    public String call(String value, String str, Integer pos, Integer length) {
        if (value == null || StringUtils.isBlank(str) || length == null || length < 1) {
            return value;
        }
        int vlen = (length - value.length()) / str.length() + 1;
        String val = value;
        if (vlen > 0) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < vlen; i++) {
                sb.append(str);
            }
            val = pos == 1 ? val + sb.toString() : sb.append(val).toString();
        }
        return pos == 1 ? val.substring(0, length) : val.substring(val.length() - length);
    }

    public static void main(String[] args) {
        FiledFillUdf udf = new FiledFillUdf();
        String vc = udf.call("123", "000", 0, 4);
        System.err.println(vc);

    }
}
