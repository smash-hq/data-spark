package com.wk.data.spark.infrastructure.util.quality;

import org.apache.spark.util.AccumulatorV2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Author: smash_hq
 * @Date: 2022/1/6 14:42
 * @Description: 重写累加器
 * @Version v1.0
 */

public class CaseAccumulatorV2 extends AccumulatorV2<CaseOpera, List<CaseOpera>> {
    protected List<CaseOpera> list = new ArrayList<>();

    @Override
    public boolean isZero() {
        return list.isEmpty();
    }

    @Override
    public AccumulatorV2<CaseOpera, List<CaseOpera>> copy() {
        return new CaseAccumulatorV2();
    }

    @Override
    public void reset() {
        list = new ArrayList<>();
    }

    @Override
    public void add(CaseOpera v) {
        list.add(v);
    }

    @Override
    public void merge(AccumulatorV2<CaseOpera, List<CaseOpera>> other) {
        list.addAll(other.value());
    }

    @Override
    public List<CaseOpera> value() {
        return list;
    }
}
