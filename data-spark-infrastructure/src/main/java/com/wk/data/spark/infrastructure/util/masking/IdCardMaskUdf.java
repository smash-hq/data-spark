package com.wk.data.spark.infrastructure.util.masking;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.sql.api.java.UDF2;

/**
 * @Author: smash_hq
 * @Date: 2021/12/1 9:12
 * @Description: 身份证脱敏
 * @Version v1.0
 */

public class IdCardMaskUdf implements UDF2<String, Integer, String> {

    /**
     * @param id
     * @param i  1-> 15位、18位身份证出身年月脱敏
     *           2-> 15位、18位身份证出身年月+1脱敏
     * @return
     * @throws Exception
     */
    @Override
    public String call(String id, Integer i) {
        if (StringUtils.isEmpty(id)) {
            return id;
        }
        int length = id.length();
        int numLength = 16;
        switch (i) {
            case 1:
                if (length <= numLength) {
                    id = id.replaceAll("(\\w{6})\\w*(\\w{3})", "$1******$2");
                } else {
                    id = id.replaceAll("(\\w{6})\\w*(\\w{4})", "$1********$2");
                }
                return id;
            case 2:
                if (length <= numLength) {
                    id = id.replaceAll("(\\w{6})\\w*(\\w{2})", "$1*******$2");
                } else {
                    id = id.replaceAll("(\\w{6})\\w*(\\w{3})", "$1*********$2");
                }
                return id;
            default:
                break;
        }
        return id;
    }

    public static void main(String[] args) {
        String id = "532sdsadfsffsd";
        System.out.println("身份证长度为：" + id.length());
        System.out.println(id.replaceAll("(\\w{6})\\w*(\\w{4})", "$1********$2"));
    }
}
