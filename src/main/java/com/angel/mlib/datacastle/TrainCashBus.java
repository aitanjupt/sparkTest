package com.angel.mlib.datacastle;


import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.GradientBoostedTrees;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.configuration.BoostingStrategy;
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/*
spark-submit --master yarn-client --class com.angel.mlib.datacastle.TrainCashBus \
--jars lib/hbase-client-0.98.6-cdh5.3.6.jar,lib/hbase-common-0.98.6-cdh5.3.6.jar\
,lib/hbase-protocol-0.98.6-cdh5.3.6.jar,lib/hbase-server-0.98.6-cdh5.3.6.jar\
,lib/htrace-core-2.04.jar,lib/zookeeper.jar,lib/spark-mllib_2.10-1.5.2.jar\
,lib/spark-core_2.10-1.5.2.jar,lib/hive-exec-0.13.1-cdh5.3.6.jar\
,lib/hive-serde-0.13.1-cdh5.3.6.jar \
spark-test-1.0.jar
 */
public class TrainCashBus implements Serializable {

    private static JavaRDD<Iterable<Tuple2<Integer, String>>> readCFFilds(JavaRDD<String> datatext) {
        JavaRDD<Iterable<Tuple2<Integer, String>>> cfFilds = datatext.map(new Function<String, Iterable<Tuple2<Integer, String>>>() {
            @Override
            public Iterable<Tuple2<Integer, String>> call(String s) throws Exception {
                String[] ss = s.split(",");
                double[] ds = new double[ss.length - 2];
                List<Tuple2<Integer, String>> list = new ArrayList<>();
                for (int i = 0; i < ds.length; i++) {
                    String ov = ss[i + 2];
                    if (ov.contains("\"")) {
                        String v = ov.replaceAll("\"", "");
                        list.add(new Tuple2<>(i, v));
                    }
                }
                return list;
            }
        });
        return cfFilds;
    }

    private static JavaPairRDD<Integer, Tuple2<Integer, Integer>> getCFMMRDD(JavaRDD<Iterable<Tuple2<Integer, String>>> cfFilds) {
        JavaPairRDD<Integer, Integer> cfTmp = cfFilds.flatMapToPair(new PairFlatMapFunction<Iterable<Tuple2<Integer, String>>, Integer, Integer>() {
            @Override
            public Iterable<Tuple2<Integer, Integer>> call(Iterable<Tuple2<Integer, String>> tuple2s) throws Exception {
                List<Tuple2<Integer, Integer>> list = new ArrayList<>();
                for (Tuple2<Integer, String> v : tuple2s) {
                    Integer value = Integer.parseInt(v._2);
                    list.add(new Tuple2<>(v._1, value));
                }
                return list;
            }
        });
        JavaPairRDD<Integer, Integer> cfMax = cfTmp.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer r1, Integer r2) throws Exception {
                return r1 > r2 ? r1 : r2;
            }
        });
        JavaPairRDD<Integer, Integer> cfMin = cfTmp.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer r1, Integer r2) throws Exception {
                return r1 > r2 ? r2 : r1;
            }
        });
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> cf = cfMin.join(cfMax);
        return cf;
    }

    public static double getValWithCF(int index, String s, HashMap<Integer, Tuple2<Integer, Integer>> cfmm) {
        Double d;
        if (s.equals("-1")) {
            return Double.MIN_VALUE;
        }
        if (s.contains("\"")) {
            String v = s.replaceAll("\"", "");
            int upd = 0;
            int min = cfmm.get(index)._1;
            if (min < 0) {
                upd = -min;
            }
            d = Double.parseDouble(v) + upd;
        } else {
            d = Double.parseDouble(s);
        }
        return d;
    }

    public static JavaRDD<LabeledPoint> readData(JavaRDD<String> datatext, final HashMap<Integer, Tuple2<Integer, Integer>> cfmm) {
        JavaRDD<LabeledPoint> data = datatext.map(new Function<String, LabeledPoint>() {
            @Override
            public LabeledPoint call(String s) throws Exception {
                String[] ss = s.split(",");
                double label = Double.parseDouble(ss[0]);
                double[] ds = new double[ss.length - 2];
                for (int i = 0; i < ds.length; i++) {
                    String ov = ss[i + 2];
                    Double d = getValWithCF(i, ov, cfmm);
                    if (d.equals(Double.MIN_VALUE)) {
                        continue;
                    }
                    ds[i] = d;
                }
                LabeledPoint lp = new LabeledPoint(label, Vectors.dense(ds));
                return lp;
            }
        });
        return data;
    }

    public static HashMap<Integer, Integer> getCFInfo(HashMap<Integer, Tuple2<Integer, Integer>> cfmm) {
        HashMap<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        Iterator iter = cfmm.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            Integer key = (Integer) entry.getKey();
            Integer min = ((Tuple2<Integer, Integer>) entry.getValue())._1;
            Integer max = ((Tuple2<Integer, Integer>) entry.getValue())._2;
            Integer val = min < 0 ? max - min + 1 : max + 1;
            categoricalFeaturesInfo.put(key, val);
        }
        return categoricalFeaturesInfo;
    }

    public static HashMap<Integer, Tuple2<Integer, Integer>> getCFMM(JavaRDD<String> datatext) {
        JavaRDD<Iterable<Tuple2<Integer, String>>> cfFilds = readCFFilds(datatext);
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> cfmmRDD = getCFMMRDD(cfFilds);
        HashMap<Integer, Tuple2<Integer, Integer>> cfmm = new HashMap<>();
        for (Tuple2<Integer, Tuple2<Integer, Integer>> cc : cfmmRDD.collect()) {
            cfmm.put(cc._1, cc._2);
        }
        return cfmm;
    }

    public static RandomForestModel trainRandomForestClassification(JavaRDD<LabeledPoint> trainingData, HashMap<Integer, Integer> cfInfo) {
        Integer numClasses = 2;
        Integer numTrees = 30; // Use more in practice.
        String featureSubsetStrategy = "auto"; // Let the algorithm choose.
        String impurity = "gini";
        Integer maxDepth = 5;
        Integer maxBins = 32;
        for (Integer i : cfInfo.values()) {
            maxBins = maxBins >= i ? maxBins : i;
        }
        Integer seed = 12345;

        final RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses,
                cfInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins,
                seed);
        return model;
    }

    public static RandomForestModel trainRandomForestRegression(JavaRDD<LabeledPoint> trainingData, Map<Integer, Integer> categoricalFeaturesInfo) {
        Integer numTrees = 50; // Use more in practice. 30
        String featureSubsetStrategy = "onethird"; // Let the algorithm choose. auto
        String impurity = "variance";
        Integer maxDepth = 8;//4
        Integer maxBins = 32;
        for (Integer i : categoricalFeaturesInfo.values()) {
            maxBins = maxBins >= i ? maxBins : i;
        }
        Integer seed = 12345;

        final RandomForestModel model = RandomForest.trainRegressor(trainingData,
                categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, seed);
        return model;
    }

    public static GradientBoostedTreesModel trainGBTSClassification(JavaRDD<LabeledPoint> trainingData) {
        // Train a GradientBoostedTrees model.
// The defaultParams for Classification use LogLoss by default.
        BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Classification");
        boostingStrategy.setNumIterations(3); // Note: Use more iterations in practice.
        boostingStrategy.getTreeStrategy().setNumClasses(2);
        boostingStrategy.getTreeStrategy().setMaxDepth(5);
// Empty categoricalFeaturesInfo indicates all features are continuous.
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(categoricalFeaturesInfo);

        final GradientBoostedTreesModel model =
                GradientBoostedTrees.train(trainingData, boostingStrategy);
        return model;
    }

    public static GradientBoostedTreesModel trainGBTSRegression(JavaRDD<LabeledPoint> trainingData, HashMap<Integer, Integer> cfInfo) {
        BoostingStrategy boostingStrategy = BoostingStrategy.defaultParams("Regression");
        boostingStrategy.setNumIterations(30); // Note: Use more iterations in practice. 30
        boostingStrategy.getTreeStrategy().setMaxDepth(5);
        Integer maxBins = 32;
        for (Integer i : cfInfo.values()) {
            maxBins = maxBins >= i ? maxBins : i;
        }
        boostingStrategy.getTreeStrategy().setMaxBins(maxBins);
        boostingStrategy.treeStrategy().setCategoricalFeaturesInfo(cfInfo);

        final GradientBoostedTreesModel model =
                GradientBoostedTrees.train(trainingData, boostingStrategy);
        return model;
    }

}