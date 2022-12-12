package com.wk.data.spark.infrastructure.util;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;

/**
 * @Author: smash_hq
 * @Date: 2021/9/27 13:26
 * @Description: 将Object转换为Document
 * @Version v1.0
 */

public class Object2DocUtil {

    private Object2DocUtil() {
        Object2DocUtil object2DocUtil = new Object2DocUtil();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Object2DocUtil.class);

    public static <T> Document toDoc(T bean) {
        Document document = new Document();
        Field[] fields = bean.getClass().getDeclaredFields();
        for (Field field : fields) {
            String key = field.getName();
            boolean accessFlag = field.isAccessible();
            field.setAccessible(true);
            try {
                Object value = field.get(bean);
                document.put(key, value);
            } catch (IllegalAccessException e) {
                LOGGER.error("获取对象信息：", e);
            } finally {
                field.setAccessible(accessFlag);
            }
        }
        return document;
    }

}
