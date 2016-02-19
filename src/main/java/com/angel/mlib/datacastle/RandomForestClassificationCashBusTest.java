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
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import org.apache.spark.mllib.util.MLUtils;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

/*
spark-submit --master yarn-client --class com.angel.mlib.datacastle.RandomForestClassificationCashBusTest \
--jars lib/hbase-client-0.98.6-cdh5.3.6.jar,lib/hbase-common-0.98.6-cdh5.3.6.jar\
,lib/hbase-protocol-0.98.6-cdh5.3.6.jar,lib/hbase-server-0.98.6-cdh5.3.6.jar\
,lib/htrace-core-2.04.jar,lib/zookeeper.jar,lib/spark-mllib_2.10-1.5.2.jar\
,lib/spark-core_2.10-1.5.2.jar,lib/hive-exec-0.13.1-cdh5.3.6.jar\
,lib/hive-serde-0.13.1-cdh5.3.6.jar \
spark-test-1.0.jar
 */
public class RandomForestClassificationCashBusTest implements Serializable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("RandomForestClassificationCashBusTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);
        // Load and parse the data
        String datapath = "/dw_ext/mllib/CashBus.test";
        JavaRDD<String> text = jsc.textFile(datapath);

        JavaRDD<LabeledPoint> data = text.map(new Function<String, LabeledPoint>() {
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

// Split the data into training and test sets (30% held out for testing)
        JavaRDD<LabeledPoint>[] splits = data.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<LabeledPoint> trainingData = splits[0];
        JavaRDD<LabeledPoint> testData = splits[1];

// Train a RandomForest model.
// Empty categoricalFeaturesInfo indicates all features are continuous.
        Integer numClasses = 2;
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        Integer numTrees = 3; // Use more in practice.
        String featureSubsetStrategy = "auto"; // Let the algorithm choose.
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        Integer seed = 12345;

        final RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
                seed);

// Evaluate model on test instances and compute test error
        JavaPairRDD<Double, Double> predictionAndLabel =
                testData.mapToPair(new PairFunction<LabeledPoint, Double, Double>() {
                    @Override
                    public Tuple2<Double, Double> call(LabeledPoint p) {
                        return new Tuple2<Double, Double>(model.predict(p.features()), p.label());
                    }
                });
        Double testErr =
                1.0 * predictionAndLabel.filter(new Function<Tuple2<Double, Double>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<Double, Double> pl) {
                        return !pl._1().equals(pl._2());
                    }
                }).count() / testData.count();
        System.out.println("Test Error: " + testErr);
        System.out.println("Learned classification forest model:\n" + model.toDebugString());

    }
}