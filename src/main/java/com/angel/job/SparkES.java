package com.angel.job;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;

import java.util.Map;

/**
 * Created by dell on 2015/9/29.
 */
public class SparkES {

    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("JavaSparkES");
//        JavaSparkContext jsc = new JavaSparkContext(conf);
//
//        JavaPairRDD<String, Map<String, Object>> esRDD = JavaEsSpark.esRDD(jsc, "xkeshi_stat_transaction/xkeshi_stat_transaction");
//        System.out.println(esRDD.count());
//        SparkContext sc = new SparkContext(conf);
//        SQLContext sqlContext = new SQLContext(sc);
//        DataFrame df = JavaEsSparkSQL.esDF(sqlContext, "xkeshi_stat_transaction/xkeshi_stat_transaction");
//        df.show();
//        df.registerTempTable("angel");
//        sqlContext.sql("SELECT COUNT(1) FROM angel");
//        df.show();
    }
}
