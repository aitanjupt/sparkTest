package com.angel.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

/**
 * Created by dell on 2015/8/12.
 */
public class SparkSql {

    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("SparkSqlTest");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
//        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(null);

    }
}
