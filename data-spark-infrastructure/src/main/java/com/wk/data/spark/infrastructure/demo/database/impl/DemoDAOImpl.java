//package com.wk.data.spark.infrastructure.demo.database.impl;
//
//import com.wk.data.spark.facade.demo.query.DemoPageQuery;
//import com.wk.data.spark.facade.demo.query.DemoQuery;
//import com.wk.data.spark.infrastructure.demo.database.DO.DemoDO;
//import com.wk.data.spark.infrastructure.demo.database.DemoDAO;
//import org.springframework.stereotype.Service;
//
//import java.util.ArrayList;
//import java.util.Date;
//import java.util.List;
//import java.util.UUID;
//
///**
// * @Classname DemoDAOImpl
// * @Description DEMO数据库查询对象，若采用annotations数据库查询方式，如@Mapper，则可不用其实现类
// * @Date 2021/7/8 5:37 下午
// * @Created by fengwei.cfw
// */
//@Service
//public class DemoDAOImpl implements DemoDAO {
//    @Override
//    public DemoDO get(String id) {
//        return DemoDO.builder().id(id).name("name").desc("demo").gmtCreate(new Date()).build();
//    }
//
//    @Override
//    public List<DemoDO> query(DemoQuery query) {
//        //TODO 根据各自选择的数据库类型，进行实际实现
//        List<DemoDO> demoDOS = new ArrayList<>();
//        demoDOS.add(DemoDO.builder().id(UUID.randomUUID().toString()).name("name1").desc("demo1").gmtCreate(new Date()).build());
//        demoDOS.add(DemoDO.builder().id(UUID.randomUUID().toString()).name("name2").desc("demo2").gmtCreate(new Date()).build());
//        return demoDOS;
//    }
//
//    @Override
//    public List<DemoDO> query(DemoPageQuery query) {
//        //TODO 根据各自选择的数据库类型，进行实际实现
//        List<DemoDO> demoDOS = new ArrayList<>();
//        demoDOS.add(DemoDO.builder().id(UUID.randomUUID().toString()).name("name1").desc("demo1").gmtCreate(new Date()).build());
//        demoDOS.add(DemoDO.builder().id(UUID.randomUUID().toString()).name("name2").desc("demo2").gmtCreate(new Date()).build());
//        return demoDOS;
//    }
//
//    @Override
//    public int count(DemoPageQuery query) {
//        int total = 2;//TODO 根据数据库中实际数量
//        return total;
//    }
//}