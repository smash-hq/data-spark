//package com.wk.data.spark.service.demo.executor.impl;
//
//import com.wk.data.spark.domain.demo.gateway.ModelGateway;
//import com.wk.data.spark.domain.demo.model.DemoModel;
//import com.wk.data.spark.domain.demo.model.DemoxModel;
//import com.wk.data.spark.facade.demo.dto.DemoDTO;
//import com.wk.data.spark.facade.demo.query.DemoPageQuery;
//import com.wk.data.spark.facade.demo.query.DemoQuery;
//import com.wk.data.spark.infrastructure.demo.database.DO.DemoDO;
//import com.wk.data.spark.infrastructure.demo.database.DemoDAO;
//import com.wk.data.spark.service.demo.executor.DemoService;
//import com.vcolco.components.basis.result.MultiResult;
//import com.vcolco.components.basis.result.PageResult;
//import com.vcolco.components.basis.result.SingleResult;
//import ma.glasnost.orika.MapperFactory;
//import ma.glasnost.orika.impl.DefaultMapperFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.util.List;
//
///**
// * @Classname DemoServiceImpl
// * @Description demo service
// * @Date 2021/5/27 4:56 下午
// * @Created by fengwei.cfw
// */
//@Service
//public class DemoServiceImpl implements DemoService {
//
//    @Autowired
//    private DemoService demoService;
//
//    @Autowired
//    private ModelGateway modelGateway;
//
//    @Autowired
//    private DemoDAO demoDAO;
//
//    private static final MapperFactory demoDO_DTO_mapperFactory = new DefaultMapperFactory.Builder().mapNulls(false).build();
//
//    static {
//        demoDO_DTO_mapperFactory.classMap(DemoDO.class, DemoDTO.class).byDefault().register();
//    }
//
//    @Override
//    public String sayHello() {
//        DemoModel demoModel = DemoModel.builder().id("123").name("张三").build();
//        return demoModel.sayHello();
//    }
//
//    @Override
//    public String combineSayHello() {
//        DemoModel demoModel = DemoModel.builder().id("123").name("张三").build();
//        DemoxModel demoxModel = DemoxModel.builder().id("234").name("李四").build();
//        return modelGateway.combine(demoModel, demoxModel);
//    }
//
//    @Override
//    public SingleResult<DemoDTO> get(String id) {
//        DemoDO demoDO = demoDAO.get(id);
//        DemoDTO demoDTO = demoDO_DTO_mapperFactory.getMapperFacade().map(demoDO, DemoDTO.class);
//        return SingleResult.of(demoDTO);
//    }
//
//    @Override
//    public MultiResult<DemoDTO> query(DemoQuery query) {
//        List<DemoDO> demoDOS = demoDAO.query(query);
//        List<DemoDTO> demoDTOs = demoDO_DTO_mapperFactory.getMapperFacade().mapAsList(demoDOS, DemoDTO.class);
//        return MultiResult.of(demoDTOs);
//    }
//
//    @Override
//    public PageResult<DemoDTO> query(DemoPageQuery query) {
//        List<DemoDO> demoDOS = demoDAO.query(query);
//        int total = demoDAO.count(query);
//        List<DemoDTO> demoDTOs = demoDO_DTO_mapperFactory.getMapperFacade().mapAsList(demoDOS, DemoDTO.class);
//        return PageResult.of(query, total, demoDTOs);
//    }
//}
