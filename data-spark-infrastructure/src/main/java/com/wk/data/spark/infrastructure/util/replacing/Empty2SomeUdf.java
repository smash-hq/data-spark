package com.wk.data.spark.infrastructure.util.replacing;

import org.apache.spark.sql.api.java.UDF2;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Map;

/**
 * @Author: smash_hq
 * @Date: 2021/11/29 15:24
 * @Description: 空值替换为指定值
 * @Version v1.0
 */

public class Empty2SomeUdf<T> implements UDF2<T, String, String> {
    /**
     * functions.expr("generateCardIdUDF(name,'')")
     *
     * @param t
     * @param s
     * @return
     * @throws Exception
     */
    @Override
    public String call(T t, String s) {
        if (isEmpty(t)) {
            return s;
        }
        return t.toString();
    }

    public boolean isEmpty(T object) {
        if (object == null) {
            return true;
        }
        if (object instanceof CharSequence) {
            return ((CharSequence) object).length() == 0;
        }
        if (object.getClass().isArray()) {
            return Array.getLength(object) == 0;
        }
        if (object instanceof Collection<?>) {
            return ((Collection<?>) object).isEmpty();
        }
        if (object instanceof Map<?, ?>) {
            return ((Map<?, ?>) object).isEmpty();
        }
        return false;
    }
}
