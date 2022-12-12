package com.wk.data.spark.service.analysis.executor.impl;

import com.wk.data.spark.service.analysis.executor.ABCAnalysisService;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.classification.NaiveBayes;
import org.apache.spark.ml.classification.NaiveBayesModel;
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator;
import org.apache.spark.ml.feature.LabeledPoint;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.types.DataTypes.*;

/**
 * @program: data-spark-job
 * @description: ABC分类分析
 * @author: gwl
 * @create: 2022-07-25 13:41
 **/
public class ABCAnalysisServiceImpl implements ABCAnalysisService {

    public static void main(String[] args) {
        //1、创建一个SparkSession
        SparkSession spark = SparkSession.builder().appName("NaiveBayes").master("local")
                .getOrCreate();

        //2、加载训练数据样本
        List<Row> data = Arrays.asList(
                RowFactory.create(0, 0.22221, 0.5, 0),
                RowFactory.create(0, 0.1122122, 0.5, 0),
                RowFactory.create(0, 0.1112222, 0.5, 0),
                RowFactory.create(0, 0.112222, 0.5, 0),
                RowFactory.create(0, 0.123333, 0.5, 0),
                RowFactory.create(0, 0.1232323, 0.5, 0),
                RowFactory.create(1, 100.09988, 1.5, 0),
                RowFactory.create(1, 100.33777, 1.5, 0),
                RowFactory.create(1, 100.334d, 1.0, 0),
                RowFactory.create(1, 100d, 1.0, 0),
                RowFactory.create(1, 100d, 1.0, 0),
                RowFactory.create(1, 100d, 1.0, 0),
                RowFactory.create(2, 25.33336, 4.0, 2),
                RowFactory.create(2, 26.223323, 4.5, 3),
                RowFactory.create(2, 27.42112, 4.5, 3),
                RowFactory.create(2, 28.2122, 4.5, 3),
                RowFactory.create(2, 22.2, 4.5, 3),
                RowFactory.create(2, 24.2, 4.5, 3),
                RowFactory.create(2, 22.1, 5.5, 1),
                RowFactory.create(3, 320.0, 9d, 2),
                RowFactory.create(3, 320.0, 9d, 2),
                RowFactory.create(3, 311.0, 9d, 2),
                RowFactory.create(3, 309.0, 9.6, 0),
                RowFactory.create(3, 307.0, 9.6, 0),
                RowFactory.create(3, 300.1, 9.9, 1)

        );
        StructType schema = new StructType(new StructField[]{
                createStructField("id", IntegerType, false),
                createStructField("code", DoubleType, false),
                createStructField("tt", DoubleType, false),
                createStructField("nn", IntegerType, false)
        });
        Dataset<Row> train = spark.createDataFrame(data, schema);
        //3、把数据样本转换成向量
        Dataset<LabeledPoint> map = train.map((MapFunction<Row, LabeledPoint>) row -> {
            double label = Double.valueOf(row.getInt(0));
            Vector vector = Vectors.dense(new double[]{row.getDouble(1), row.getDouble(2), row.getInt(3)});
            return LabeledPoint.apply(label,vector);
        }, Encoders.bean(LabeledPoint.class));

        Dataset<Row> dataFrame = spark.createDataFrame(map.javaRDD(), LabeledPoint.class);


        Dataset<Row>[] randomSplit = dataFrame.randomSplit(new double[]{0.8, .02}, 123L);
        Dataset<Row> trainDs = randomSplit[0];
        Dataset<Row> testDs = randomSplit[1];
        trainDs.show(false);
        testDs.show(false);
        //贝叶斯算法  label,features
        NaiveBayes naiveBayes = new NaiveBayes()
                .setSmoothing(0.01d);
        NaiveBayesModel bayesModel = naiveBayes.fit(trainDs);
        Dataset<Row> prediction = bayesModel.transform(testDs);
        Object obj = prediction.take(1);
        System.out.println(obj);
        prediction.show();


        MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator()
                .setLabelCol("label")
                .setPredictionCol("prediction")
                .setMetricName("accuracy");
        double evaluate = evaluator.evaluate(prediction);
        System.out.println("准确率:" + evaluate);


//        //3、通过tf-idf计算数据样本中的词频
//        //word frequency count
//        HashingTF hashingTF = new HashingTF().setNumFeatures(500000).setInputCol("text").setOutputCol("rawFeatures");
//        Dataset<Row> featurizedData  = hashingTF.transform(train);
//        //count tf-idf
//        IDF idf = new IDF().setInputCol("rawFeatures").setOutputCol("features");
//        IDFModel idfModel = idf.fit(featurizedData);
//        Dataset<Row> rescaledData = idfModel.transform(featurizedData);
//
//        //4、把数据样本转换成向量
//        JavaRDD<LabeledPoint> trainDataRdd = rescaledData.select("category", "features").javaRDD().map(v1 -> {
//            Double category = v1.getAs("category");
//            SparseVector features = v1.getAs("features");
//            Vector featuresVector = Vectors.dense(features.toArray());
//            return new LabeledPoint(Double.valueOf(category),featuresVector);
//        });
//
//        System.out.println("Start training...");
//        //调用朴素贝叶斯算法，传入向量数据训练模型
//        NaiveBayesModel model  = NaiveBayes.train(trainDataRdd.rdd());
//        //save model
//        model.save(spark.sparkContext(), DataFactory.MODEL_PATH);
//        //save tf
//        hashingTF.save(DataFactory.TF_PATH);
//        //save idf
//        idfModel.save(DataFactory.IDF_PATH);
//
//        System.out.println("train successfully !");
//        System.out.println("=======================================================");
//        System.out.println("modelPath:"+DataFactory.MODEL_PATH);
//        System.out.println("tfPath:"+DataFactory.TF_PATH);
//        System.out.println("idfPath:"+DataFactory.IDF_PATH);
//        System.outprintln("=======================================================");

        spark.close();
    }
}
