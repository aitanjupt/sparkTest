package com.angel.mlib;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.stat.Statistics;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

/*
spark-submit --master yarn-client --class com.angel.mlib.CorrelationsWeatherTest \
--jars lib/hbase-client-0.98.6-cdh5.3.6.jar,lib/hbase-common-0.98.6-cdh5.3.6.jar\
,lib/hbase-protocol-0.98.6-cdh5.3.6.jar,lib/hbase-server-0.98.6-cdh5.3.6.jar\
,lib/htrace-core-2.04.jar,lib/zookeeper.jar,lib/spark-mllib_2.10-1.5.2.jar\
,lib/spark-core_2.10-1.5.2.jar,lib/hive-exec-0.13.1-cdh5.3.6.jar\
,lib/hive-serde-0.13.1-cdh5.3.6.jar \
spark-test-1.0.jar
 */
public class CorrelationsWeatherTest implements Serializable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("CorrelationsWeatherTest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load and parse the data
        String path = "/dw_ext/mllib/CorrelationsWeather.test";
        JavaRDD<String> data = sc.textFile(path);

        JavaPairRDD<Object, Object> dataParsed = data.mapToPair(new PairFunction<String, Object, Object>() {
            @Override
            public Tuple2<Object, Object> call(String s) throws Exception {
                String[] parts = s.split("\t");
                Double x = Double.parseDouble(parts[0]);
                Double y = Double.parseDouble(parts[1]);
                Tuple2 tuple2 = new Tuple2(x, y);
                return tuple2;
            }
        });

        JavaDoubleRDD seriesX = JavaDoubleRDD.fromRDD(dataParsed.keys().rdd());
        JavaDoubleRDD seriesY = JavaDoubleRDD.fromRDD(dataParsed.values().rdd());

        Double correlationPearson = Statistics.corr(seriesX.srdd(), seriesY.srdd(), "pearson");
        Double correlationSpearman = Statistics.corr(seriesX.srdd(), seriesY.srdd(), "spearman");

        System.out.println(">>>>>>>correlationPearson:" + correlationPearson);
        System.out.println(">>>>>>>correlationSpearman:" + correlationSpearman);

    }
}