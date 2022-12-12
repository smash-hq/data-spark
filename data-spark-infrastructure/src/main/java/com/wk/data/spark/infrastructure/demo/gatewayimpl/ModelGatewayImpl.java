//package com.wk.data.spark.infrastructure.demo.gatewayimpl;
//
//import com.wk.data.spark.domain.demo.gateway.ModelGateway;
//import com.wk.data.spark.domain.demo.model.DemoModel;
//import com.wk.data.spark.domain.demo.model.DemoxModel;
//import org.springframework.stereotype.Service;
//
///**
// * @Classname ModelGatewayImpl
// * @Description TODO
// * @Date 2021/6/21 9:22 上午
// * @Created by fengwei.cfw
// */
//@Service
//public class ModelGatewayImpl implements ModelGateway {
//    @Override
//    public String combine(DemoModel demoModel, DemoxModel demoxModel) {
//        if (demoModel == null || demoxModel == null) {
//            return null;
//        }
//        return String.format("%s--%s", demoModel.sayHello(), demoxModel.sayHello());
//    }
//}
