//package com.wk.data.spark.infrastructure.demo.database;
//
//import com.wk.data.spark.facade.demo.query.DemoPageQuery;
//import com.wk.data.spark.facade.demo.query.DemoQuery;
//import com.wk.data.spark.infrastructure.demo.database.DO.DemoDO;
//
//import java.util.List;
//
///**
// * @Classname DemoDAO
// * @Description DEMO数据库查询对象，若采用annotations数据库查询方式，如@Mapper，则可不用其实现类
// * @Date 2021/7/8 5:32 下午
// * @Created by fengwei.cfw
// */
//public interface DemoDAO {
//
//    DemoDO get(String id);
//
//    List<DemoDO> query(DemoQuery query);
//
//    List<DemoDO> query(DemoPageQuery query);
//
//    int count(DemoPageQuery query);
//}
