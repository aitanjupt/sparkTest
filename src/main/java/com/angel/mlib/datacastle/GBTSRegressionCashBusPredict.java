package com.angel.mlib.datacastle;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import scala.Tuple2;

import java.io.Serializable;

/*
spark-submit --master yarn-client --class com.angel.mlib.datacastle.GBTSRegressionCashBusPredict \
--jars lib/hbase-client-0.98.6-cdh5.3.6.jar,lib/hbase-common-0.98.6-cdh5.3.6.jar\
,lib/hbase-protocol-0.98.6-cdh5.3.6.jar,lib/hbase-server-0.98.6-cdh5.3.6.jar\
,lib/htrace-core-2.04.jar,lib/zookeeper.jar,lib/spark-mllib_2.10-1.5.2.jar\
,lib/spark-core_2.10-1.5.2.jar,lib/hive-exec-0.13.1-cdh5.3.6.jar\
,lib/hive-serde-0.13.1-cdh5.3.6.jar \
spark-test-1.0.jar
 */
public class GBTSRegressionCashBusPredict implements Serializable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("GBTSRegressionCashBusPredict");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        // Load and parse the data
        String trainingDatapath = "/dw_ext/mllib/CashBus.training";
        String testDatapath = "/dw_ext/mllib/CashBus.test";
        JavaRDD<String> trainingDatatext = jsc.textFile(trainingDatapath);
        JavaRDD<String> testDatatext = jsc.textFile(testDatapath);

        JavaRDD<LabeledPoint> trainingData = trainingDatatext.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String s) throws Exception {
                String[] ss = s.split(",");
                double label = Double.parseDouble(ss[0]);
                double[] ds = new double[ss.length - 2];
                for (int i = 0; i < ds.length; i++) {
                    ds[i] = Double.parseDouble(ss[i + 2]);
                }
                LabeledPoint lp = new LabeledPoint(label, Vectors.dense(ds));
                return lp;
            }
        });

        final GradientBoostedTreesModel model = TrainCashBus.trainGBTSRegression(trainingData);

        JavaPairRDD<Long, Vector> testData = testDatatext.mapToPair(new PairFunction<String, Long, Vector>() {
            @Override
            public Tuple2<Long, Vector> call(String s) throws Exception {
                String[] ss = s.split(",");
                Long uid = Long.parseLong(ss[0]);
                double[] ds = new double[ss.length - 1];
                for (int i = 0; i < ds.length; i++) {
                    ds[i] = Double.parseDouble(ss[i + 1]);
                }
                Vector vector = Vectors.dense(ds);
                Tuple2<Long, Vector> ret = new Tuple2<>(uid, vector);
                return ret;
            }
        });

        JavaPairRDD<Long, Double> userAndPrediction = testData.mapValues(new Function<Vector, Double>() {
            @Override
            public Double call(Vector vector) throws Exception {
                Double predict = model.predict(vector);
                return predict;
            }
        });

        userAndPrediction.repartition(1).saveAsTextFile("/dw_ext/mllib/CashBus.test.predict");

        GBTSRegressionCashBusTest.test(trainingData, model);
    }
}