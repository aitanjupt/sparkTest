package com.angel.mlib.datacastle;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.classification.SVMModel;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import scala.Tuple2;

import java.util.HashMap;

/*
spark-submit --master yarn-client --class com.angel.mlib.datacastle.SVMCashBusTest \
--jars lib/hbase-client-0.98.6-cdh5.3.6.jar,lib/hbase-common-0.98.6-cdh5.3.6.jar\
,lib/hbase-protocol-0.98.6-cdh5.3.6.jar,lib/hbase-server-0.98.6-cdh5.3.6.jar\
,lib/htrace-core-2.04.jar,lib/zookeeper.jar,lib/spark-mllib_2.10-1.5.2.jar\
,lib/spark-core_2.10-1.5.2.jar,lib/hive-exec-0.13.1-cdh5.3.6.jar\
,lib/hive-serde-0.13.1-cdh5.3.6.jar \
spark-test-1.0.jar
 */
public class SVMCashBusTest {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("SVMCashBusTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        // Load and parse the data
        String datapath = "/dw_ext/mllib/CashBus.training";
        JavaRDD<String> text = jsc.textFile(datapath);

        HashMap<Integer, Tuple2<Integer, Integer>> cfmm = TrainCashBus.getCFMM(text);

        JavaRDD<LabeledPoint> data = TrainCashBus.readData(text, cfmm);

//        HashMap<Integer, Integer> cfInfo = TrainCashBus.getCFInfo(cfmm);

        // Split the data into training and test sets (30% held out for testing)
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

        int numIterations = 100;
        final SVMModel model = SVMWithSGD.train(trainingData.rdd(), numIterations);
        model.clearThreshold();

        test(testData, model);

    }

    private static void test(JavaRDD<LabeledPoint> testData, final SVMModel model) {
// Evaluate model on test instances and compute test error
        JavaPairRDD<Object, Object> scoreAndLabels =
                testData.mapToPair(new PairFunction<LabeledPoint, Object, Object>() {
                    @Override
                    public Tuple2<Object, Object> call(LabeledPoint p) {
                        return new Tuple2<Object, Object>(model.predict(p.features()), p.label());
                    }
                });

        BinaryClassificationMetrics bcm = new BinaryClassificationMetrics(scoreAndLabels.rdd());
        double auROC = bcm.areaUnderROC();

        System.out.println("Area under ROC = " + auROC);

//        System.out.println("Learned classification forest model:\n" + model.toDebugString());
    }
}
