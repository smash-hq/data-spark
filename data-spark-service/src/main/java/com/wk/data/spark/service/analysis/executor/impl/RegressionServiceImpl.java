package com.wk.data.spark.service.analysis.executor.impl;

import com.google.common.collect.Lists;
import com.wk.data.spark.service.analysis.executor.RegressionService;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
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

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @program: data-spark-job
 * @description: 回归分析
 * @author: gwl
 * @create: 2022-07-25 13:27
 **/
public class RegressionServiceImpl implements RegressionService {


    public static void main(String[] args) throws IOException {
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
                .withColumn("price", rowDataset.col("priceD").cast(DataTypes.DoubleType));

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
        trainingSummary.predictions().show();
//        lrModel.transform(rowDataset).show();
//        Dataset<Row> dataset = trainingSummary.predictions().select("label", "price", "features","prediction");
//        dataset.show(false);
        logisticRegression(sparkSession);
        sparkSession.close();
    }

    private static void logisticRegression(SparkSession sparkSession) throws IOException {
        List<Row> list = new ArrayList<>();
        list.add(RowFactory.create(34.62365962451697,78.0246928153624,0));
        list.add(RowFactory.create(30.28671076822607,43.89499752400101,0));
        list.add(RowFactory.create(35.84740876993872,72.90219802708364,0));
        list.add(RowFactory.create(60.18259938620976,86.30855209546826,1));
        list.add(RowFactory.create(79.0327360507101,75.3443764369103,1));
        list.add(RowFactory.create(45.08327747668339,56.3163717815305,0));
        list.add(RowFactory.create(61.10666453684766,96.51142588489624,1));
        list.add(RowFactory.create(75.02474556738889,46.55401354116538,1));
        list.add(RowFactory.create(76.09878670226257,87.42056971926803,1));
        list.add(RowFactory.create(84.43281996120035,43.53339331072109,1));
        list.add(RowFactory.create(95.86155507093572,38.22527805795094,0));
        list.add(RowFactory.create(75.01365838958247,30.60326323428011,0));
        list.add(RowFactory.create(82.30705337399482,76.48196330235604,1));
        list.add(RowFactory.create(34.21206097786789,44.20952859866288,0));
        list.add(RowFactory.create(77.9240914545704,68.9723599933059,1));
        list.add(RowFactory.create(62.27101367004632,69.95445795447587,1));
        list.add(RowFactory.create(80.1901807509566,44.82162893218353,1));
        list.add(RowFactory.create(93.114388797442,38.80067033713209,0));
        list.add(RowFactory.create(61.83020602312595,50.25610789244621,0));
        list.add(RowFactory.create(38.78580379679423,64.99568095539578,0));
        list.add(RowFactory.create(61.379289447425,72.80788731317097,1));
        list.add(RowFactory.create(85.40451939411645,57.05198397627122,1));
        list.add(RowFactory.create(52.10797973193984,63.12762376881715,0));
        list.add(RowFactory.create(52.04540476831827,69.43286012045222,1));
        list.add(RowFactory.create(40.23689373545111,71.16774802184875,0));
        list.add(RowFactory.create(54.63510555424817,52.21388588061123,0));
        list.add(RowFactory.create(33.91550010906887,98.86943574220611,0));
        list.add(RowFactory.create(64.17698887494485,80.90806058670817,1));
        list.add(RowFactory.create(74.78925295941542,41.57341522824434,0));
        list.add(RowFactory.create(34.1836400264419,75.2377203360134,0));
        list.add(RowFactory.create(83.90239366249155,56.30804621605327,1));
        list.add(RowFactory.create(51.54772026906181,46.85629026349976,0));
        list.add(RowFactory.create(94.44336776917852,65.56892160559052,1));
        list.add(RowFactory.create(82.36875375713919,40.61825515970618,0));
        list.add(RowFactory.create(51.04775177128865,45.82270145776001,0));
        list.add(RowFactory.create(62.22267576120188,52.06099194836679,0));
        list.add(RowFactory.create(77.19303492601364,70.45820000180959,1));
        list.add(RowFactory.create(97.77159928000232,86.7278223300282,1));
        list.add(RowFactory.create(62.07306379667647,96.76882412413983,1));
        list.add(RowFactory.create(91.56497449807442,88.69629254546599,1));
        list.add(RowFactory.create(79.94481794066932,74.16311935043758,1));
        list.add(RowFactory.create(99.2725269292572,60.99903099844988,1));
        list.add(RowFactory.create(90.54671411399852,43.39060180650027,1));
        list.add(RowFactory.create(34.52451385320009,60.39634245837173,0));
        list.add(RowFactory.create(50.2864961189907,49.80453881323059,0));
        list.add(RowFactory.create(49.58667721632031,59.80895099453265,0));
        list.add(RowFactory.create(97.64563396007767,68.86157272420604,1));
        list.add(RowFactory.create(32.57720016809309,95.59854761387875,0));
        list.add(RowFactory.create(74.24869136721598,69.82457122657193,1));
        list.add(RowFactory.create(71.79646205863379,78.45356224515052,1));
        list.add(RowFactory.create(75.3956114656803,85.75993667331619,1));
        list.add(RowFactory.create(35.28611281526193,47.02051394723416,0));
        list.add(RowFactory.create(56.25381749711624,39.26147251058019,0));
        list.add(RowFactory.create(30.05882244669796,49.59297386723685,0));
        list.add(RowFactory.create(44.66826172480893,66.45008614558913,0));
        list.add(RowFactory.create(66.56089447242954,41.09209807936973,0));
        list.add(RowFactory.create(40.45755098375164,97.53518548909936,1));
        list.add(RowFactory.create(49.07256321908844,51.88321182073966,0));
        list.add(RowFactory.create(80.27957401466998,92.11606081344084,1));
        list.add(RowFactory.create(66.74671856944039,60.99139402740988,1));
        list.add(RowFactory.create(32.72283304060323,43.30717306430063,0));
        list.add(RowFactory.create(64.0393204150601,78.03168802018232,1));
        list.add(RowFactory.create(72.34649422579923,96.22759296761404,1));
        list.add(RowFactory.create(60.45788573918959,73.09499809758037,1));
        list.add(RowFactory.create(58.84095621726802,75.85844831279042,1));
        list.add(RowFactory.create(99.82785779692128,72.36925193383885,1));
        list.add(RowFactory.create(47.26426910848174,88.47586499559782,1));
        list.add(RowFactory.create(50.45815980285988,75.80985952982456,1));
        list.add(RowFactory.create(60.45555629271532,42.50840943572217,0));
        list.add(RowFactory.create(82.22666157785568,42.71987853716458,0));
        list.add(RowFactory.create(88.9138964166533,69.80378889835472,1));
        list.add(RowFactory.create(94.83450672430196,45.69430680250754,1));
        list.add(RowFactory.create(67.31925746917527,66.58935317747915,1));
        list.add(RowFactory.create(57.23870631569862,59.51428198012956,1));
        list.add(RowFactory.create(80.36675600171273,90.96014789746954,1));
        list.add(RowFactory.create(68.46852178591112,85.59430710452014,1));
        list.add(RowFactory.create(42.0754545384731,78.84478600148043,0));
        list.add(RowFactory.create(75.47770200533905,90.42453899753964,1));
        list.add(RowFactory.create(78.63542434898018,96.64742716885644,1));
        list.add(RowFactory.create(52.34800398794107,60.76950525602592,0));
        list.add(RowFactory.create(94.09433112516793,77.15910509073893,1));
        list.add(RowFactory.create(90.44855097096364,87.50879176484702,1));
        list.add(RowFactory.create(55.48216114069585,35.57070347228866,0));
        list.add(RowFactory.create(74.49269241843041,84.84513684930135,1));
        list.add(RowFactory.create(89.84580670720979,45.35828361091658,1));
        list.add(RowFactory.create(83.48916274498238,48.38028579728175,1));
        list.add(RowFactory.create(42.2617008099817,87.10385094025457,1));
        list.add(RowFactory.create(99.31500880510394,68.77540947206617,1));
        list.add(RowFactory.create(55.34001756003703,64.9319380069486,1));
        list.add(RowFactory.create(74.77589300092767,89.52981289513276,1));
        list.add(RowFactory.create(69.36458875970939,97.71869196188608,1));
        list.add(RowFactory.create(39.53833914367223,76.03681085115882,0));
        list.add(RowFactory.create(53.9710521485623,89.20735013750205,1));
        list.add(RowFactory.create(69.07014406283025,52.74046973016765,1));
        list.add(RowFactory.create(67.94685547711617,46.67857410673128,0));
        StructType structType = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("score1", DataTypes.DoubleType, false),
                DataTypes.createStructField("score2", DataTypes.DoubleType, false),
                DataTypes.createStructField("result", DataTypes.IntegerType, false)
                )
        );

        Dataset<Row> trans = sparkSession.createDataFrame(list, structType);
        // 转换成特征向量的列
        String[] transClos = new String[]{"score1", "score2"};
        // 转化成向量
        VectorAssembler assembler = new VectorAssembler().setInputCols(transClos).setOutputCol("features");
        // 得到特征向量的dataFrame
        Dataset<Row> featureDf = assembler.transform(trans);

        // 根据result列新建一个表前列
        StringIndexer indexer = new StringIndexer().setInputCol("result").setOutputCol("label");
        Dataset<Row> lableDf = indexer.fit(featureDf).transform(featureDf);

        int seed = 5043;
        // 70%的数据用于训练模型，30%用于测试
        Dataset<Row>[] randomSplit = lableDf.randomSplit(new double[]{0.7, 0.3}, seed);
        Dataset<Row> trainData = randomSplit[0];
        Dataset<Row> testData = randomSplit[1];
        // 建立回归模型，用训练集数据开始训练
        LogisticRegression regression = new LogisticRegression()
                .setMaxIter(100) // 设置最大的迭代次数，默认100次
                .setRegParam(0.02)  // 设置正则化参数，默认0.0
                // 设置弹性网混合参数，默认0.0。 0->L2（ridge regression岭回归）;
                // 1->L1（Lasso套索）;(0,1)->L1和L2的组合；与 huber 损失拟合仅支持 None 和 L2 正则化，因此如果此参数为非零值，则会引发异常
                .setElasticNetParam(0.8);
        LogisticRegressionModel regressionModel = regression.fit(trainData);
        /*
          使用测试数据集预测得到的 DataFrame，添加三个新的列
          1、rawPrediction  通常是直接概率
          2、probability    每个类的条件概率
          3、prediction     rawPrediction  -  via 的统计结果
         */
        Dataset<Row> predictionDf = regressionModel.transform(testData);
        // ROC 下面积的评估模型
        BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator()
                .setLabelCol("label")
                .setRawPredictionCol("prediction")
                .setMetricName("areaUnderROC");
        // 测量精度
        double accuracy = evaluator.evaluate(predictionDf);
        System.err.println("预测的精度为：" +accuracy);
        // 保存模型
//        regressionModel.write().overwrite().save("D://score-model");
        // 加载模型
//        LogisticRegressionModel regressionModelLoaded = LogisticRegressionModel.read().load("D://score-model");
        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create(70.66150955499435, 92.92713789364831));
        rows.add(RowFactory.create(76.97878372747498, 47.57596364975532));
        rows.add(RowFactory.create(67.37202754570876, 42.83843832029179));
        rows.add(RowFactory.create(89.67677575072079, 65.79936592745237));

        StructType st = DataTypes.createStructType(Lists.newArrayList(
                DataTypes.createStructField("score1", DataTypes.DoubleType, false),
                DataTypes.createStructField("score2", DataTypes.DoubleType, false)
                )
        );

        Dataset<Row> df = sparkSession.createDataFrame(rows, st);
        df.show();
        // 转换样本集数据集并添加特征列
        Dataset<Row> df2 = assembler.transform(df);
        df2.show();
        // 最后的结果表示预测新来学生第三次考试的及格和不及格， 0 -及格 1-不及格
        Dataset<Row> df3 = regressionModel.transform(df2);
        df3.show();
    }
}
