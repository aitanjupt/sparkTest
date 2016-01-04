package com.angel.hadoopspark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.api.java.JavaSchemaRDD;
import org.apache.spark.sql.hive.api.java.JavaHiveContext;

/**
 * Created by dell on 2015/12/22.
 */
public class SparkHiveTest {

    public static void main(String[] args) {
        test1();
    }

    static void test1() {
        SparkConf conf = new SparkConf().setAppName("SparkHiveTest");
        JavaSparkContext sc = new JavaSparkContext(conf);

        sc.addJar("/home/mllib/lib/hive-common-0.13.1-cdh5.3.6.jar");
        // Load and parse the train data from hive 尚未成功
        JavaHiveContext hiveCtx = new JavaHiveContext(sc);


        JavaSchemaRDD trainData = hiveCtx.sql("SELECT user_id,item_id,count from mds_member_item_num where dt=20151211");
        System.out.println("trainData.count: " + trainData.count());
//        JavaRDD<Rating> trainRatings = trainData.map(
//                new Function<Row, Rating>() {
//                    public Rating call(Row row) throws Exception {
//                        return new Rating(row.getInt(0), row.getInt(1), row.getInt(2));
//                    }
//                }
//        );
//        System.out.println("trainRatings.count: " + trainRatings.count());
    }
}
