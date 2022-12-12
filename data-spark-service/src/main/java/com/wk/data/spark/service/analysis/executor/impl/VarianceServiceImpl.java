package com.wk.data.spark.service.analysis.executor.impl;

import com.google.common.collect.Lists;
import com.wk.data.spark.service.analysis.executor.VarianceService;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @program: data-spark-job
 * @description: 方差分析算法实现
 * @author: gwl
 * @create: 2022-07-25 10:08
 **/
public class VarianceServiceImpl implements VarianceService {

    @Override
    public void analysis(String[] args) {

    }

    public static void main(String[] args)  {
        SparkSession sparkSession = SparkSession.builder()
                .config("date", LocalDate.now().minusDays(1).format(DateTimeFormatter.BASIC_ISO_DATE))
                .config("spark.sql.debug.maxToStringFields", "100")
                .master("local[*]")
                .getOrCreate();
        List<Row> list = new ArrayList<>();
        list.add(RowFactory.create("1.0", "1.9", "1.0"));
        list.add(RowFactory.create("2.0", "3.1", "0.0"));
        list.add(RowFactory.create("3.0","4.0", "1.0"));
        list.add(RowFactory.create("3.5", "4.45", "0.0"));
        list.add(RowFactory.create("4.0", "5.02", "1.0"));
        list.add(RowFactory.create("9.0", "9.97", "0.0"));
        list.add(RowFactory.create("-2.0", "-0.98", "1.0"));
        StructType structType = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("labelD", DataTypes.StringType, false),
                DataTypes.createStructField("priceD", DataTypes.StringType, false),
                DataTypes.createStructField("ID", DataTypes.StringType, false)
                )
        );

        Dataset<Row> rowDataset = sparkSession.createDataFrame(list, structType);
        Dataset<Row> data = rowDataset
                //label 用来计算 系数 和 截距
                .withColumn("label", rowDataset.col("labelD").cast(DataTypes.DoubleType))
                .withColumn("price", rowDataset.col("priceD").cast(DataTypes.DoubleType))
                ;

        String[] transClos = (String[]) Arrays.asList("price").toArray();
        VectorAssembler vectorAssembler = new VectorAssembler().setInputCols(transClos).setOutputCol("features");

        Dataset<Row> training = vectorAssembler.transform(data);

        LinearRegression lr = new LinearRegression()
                .setMaxIter(10)//设置最大迭代次数，默认是100。
                .setRegParam(0.3)//设置正则化参数,默认0.0。
                .setElasticNetParam(0.8);//设置弹性网混合参数，默认0.0。 0->L2（ridge regression岭回归）;1->L1（Lasso套索）;(0,1)->L1和L2的组合；与 huber 损失拟合仅支持 None 和 L2 正则化，因此如果此参数为非零值，则会引发异常
        //训练模型
        LinearRegressionModel lrModel = lr.fit(training);
        //打印线性回归的系数和截距
        System.out.println("系数Coefficients: "+lrModel.coefficients() + "");
        System.out.println(" 截距Intercept: " + lrModel.intercept()+ "");
        //总结训练集上的模型并打印出一些指标。
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        //trainingSummary.predictions().show();
        Dataset<Row> dataset = trainingSummary.predictions().select("label", "price", "features","prediction");
        dataset.show(false);
        sparkSession.close();
    }

}
