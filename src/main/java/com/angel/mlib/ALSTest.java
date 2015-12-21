package com.angel.mlib;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.io.Serializable;

/***
 * 测试是否可以跨区域推荐
 * 用户1与其他用户的商品不在一个区域内
 */
public class ALSTest implements Serializable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Collaborative Filtering Example");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load and parse the data
        String path = "/dw_ext/20.train";
        JavaRDD<String> data = sc.textFile(path);
        JavaRDD<Rating> ratings = data.map(
                new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = s.split(",");
                        return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2]));
                    }
                }
        );
        //使用ALS训练数据建立推荐模型 Build the recommendation model using ALS
        int rank = 10;
        int numIterations = 20;
        MatrixFactorizationModel model = org.apache.spark.mllib.recommendation.ALS.train(JavaRDD.toRDD(ratings), rank, numIterations, 0.01);

        Rating[] ratings1 = model.recommendProducts(1, 10);
        for (Rating rating : ratings1) {
            System.out.println("product:" + rating.product() + " ratting:" + rating.rating());
        }
    }
}