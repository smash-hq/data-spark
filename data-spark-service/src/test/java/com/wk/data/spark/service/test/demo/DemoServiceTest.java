//package com.wk.data.spark.service.test.demo;
//
//import com.wk.data.spark.facade.demo.dto.DemoDTO;
//import com.wk.data.spark.service.demo.executor.DemoService;
//import com.wk.data.spark.service.test.ApplicationTest;
//import com.vcolco.components.basis.result.SingleResult;
//import org.junit.Assert;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.test.context.SpringBootTest;
//import org.springframework.test.context.junit4.SpringRunner;
//
///**
// * @Classname DemoServiceTest
// * @Description Demo测试用例
// * @Date 2021/7/9 9:55 上午
// * @Created by fengwei.cfw
// */
//@RunWith(SpringRunner.class)
//@SpringBootTest(classes = ApplicationTest.class)
//public class DemoServiceTest {
//
//    @Autowired
//    private DemoService demoService;
//
//    @Test
//    public void testDemoGet() {
//        SingleResult<DemoDTO> demoDTOSingleResult = demoService.get("xxxx");
//        Assert.assertNotNull(demoDTOSingleResult.getData());
//    }
//}
