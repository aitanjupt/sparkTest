package com.angel.hadoopspark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.hive.HiveContext;

/**
 * Created by dell on 2015/12/22.
 */
public class SparkHiveTest {

    public static void main(String[] args) {
        test1();
    }

    static void test1() {
        SparkConf conf = new SparkConf().setAppName("SparkHiveTest");
        SparkContext sc = new SparkContext(conf);

        // Load and parse the train data from hive 尚未成功
        HiveContext hiveContext = new org.apache.spark.sql.hive.HiveContext(sc);

        DataFrame dataFrame = hiveContext.sql("select * from mds_dm_iar_solditems where business_id = 112563");
        System.out.println("trainData.count: " + dataFrame.count());
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
