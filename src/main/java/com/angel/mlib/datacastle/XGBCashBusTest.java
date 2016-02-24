package com.angel.mlib.datacastle;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.VectorUDT;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import rotationsymmetry.sxgboost.SparkXGBoost;
import rotationsymmetry.sxgboost.SparkXGBoostModel;
import rotationsymmetry.sxgboost.loss.SquareLoss;
import scala.Tuple2;

import java.io.Serializable;
import java.util.HashMap;

/*
spark-submit --master yarn-client --class com.angel.mlib.datacastle.XGBCashBusTest \
--driver-class-path lib/spark-assembly-1.6.0-hadoop2.2.0.jar \
--jars lib/sparkxgboost-0.2.1-s_2.10.jar \
spark-test-1.0.jar
*/

//--jars lib/hbase-client-0.98.6-cdh5.3.6.jar,lib/hbase-common-0.98.6-cdh5.3.6.jar\
//        ,lib/hbase-protocol-0.98.6-cdh5.3.6.jar,lib/hbase-server-0.98.6-cdh5.3.6.jar\
//        ,lib/htrace-core-2.04.jar,lib/zookeeper.jar,lib/spark-mllib_2.10-1.6.0.jar\
//        ,lib/spark-core_2.10-1.5.2.jar,lib/hive-exec-0.13.1-cdh5.3.6.jar\
//        ,lib/hive-serde-0.13.1-cdh5.3.6.jar\
//        ,lib/sparkxgboost-0.2.1-s_2.10.jar,lib/spark-catalyst_2.10-1.6.0.jar \
public class XGBCashBusTest implements Serializable {

    private String datapath;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("XGBCashBusTest");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        SQLContext sqlContext = new SQLContext(jsc);

        // Load and parse the data
        String datapath = "/dw_ext/mllib/CashBus.training";
        JavaRDD<String> text = jsc.textFile(datapath);

        final HashMap<Integer, Tuple2<Integer, Integer>> cfmm = TrainCashBus.getCFMM(text);

        StructType schema = new StructType(new StructField[]{
                new StructField("lable", DataTypes.DoubleType, true, Metadata.empty()),
                new StructField("features", new VectorUDT(), false, Metadata.empty())
        });

        JavaRDD<Row> daat = text.map(new Function<String, Row>() {
            @Override
            public Row call(String s) throws Exception {
                String[] ss = s.split(",");
                double label = Double.parseDouble(ss[0]);
                double[] ds = new double[ss.length - 2];
                for (int i = 0; i < ds.length; i++) {
                    String ov = ss[i + 2];
                    Double d = TrainCashBus.getValWithCF(i, ov, cfmm);
                    if (d.equals(Double.MIN_VALUE)) {
                        continue;
                    }
                    ds[i] = d;
                }
                Row row = RowFactory.create(label, Vectors.dense(ds));
                return row;
            }
        });
        JavaRDD<Row>[] splits = daat.randomSplit(new double[]{0.7, 0.3});
        JavaRDD<Row> trainingData = splits[0];
        JavaRDD<Row> testData = splits[1];


        SquareLoss loss = new SquareLoss();
        SparkXGBoost sparkXGBoost = new SparkXGBoost(loss);
        sparkXGBoost.setMaxBins(36).setMaxDepth(5).setNumTrees(10);




        DataFrame trainingDataDF = sqlContext.createDataFrame(trainingData, schema);

        final SparkXGBoostModel sparkXGBoostModel = sparkXGBoost.fit(trainingDataDF);

        JavaPairRDD<Object, Object> scoreAndLabels = testData.mapToPair(new PairFunction<Row, Object, Object>() {
            @Override
            public Tuple2<Object, Object> call(Row row) throws Exception {
                Double label = row.getDouble(1);
                Vector features = (Vector) row.get(2);
                return new Tuple2<Object, Object>(sparkXGBoostModel.predict(features), label);
            }
        });

        BinaryClassificationMetrics bcm = new BinaryClassificationMetrics(scoreAndLabels.rdd());
        double auROC = bcm.areaUnderROC();

        System.out.println("Area under ROC = " + auROC);

    }
}