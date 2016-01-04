package com.angel.mlib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.classification.SVMWithSGD;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by dell on 2015/12/11.
 */
public class ALSTrainTestReal implements Serializable {

    private static int numPartitions = 10;

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Collaborative Filtering Example");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load and parse the train data from hive hive文件非压缩格式rcfile
        String realDataPath = "/dw_ext/sinaad/dm/mds/mds_member_item_num/dt=20151218/000000_0";
        JavaRDD<String> rdd_realData = sc.textFile(realDataPath);
        JavaRDD<Rating> rdd_realRating = rdd_realData.map(
                new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = s.split("\t");
                        Integer userId = Integer.parseInt(sarray[0]);
                        Integer itemId = Integer.parseInt(sarray[1]);
                        Integer num = Integer.parseInt(sarray[2]);
                        Integer maxNum = Integer.parseInt(sarray[3]);
                        Double rating = -1.0;
                        if (10 <= maxNum) {//只买一次的rating=-1
                            rating = (100.00 * num) / maxNum;
                        }
                        return new Rating(userId, itemId, rating);
                    }
                }
        );

        long rdd_realRating_count = rdd_realRating.count();

        JavaRDD<Rating> rdd_realRatingFiltered = rdd_realRating.filter(new Function<Rating, Boolean>() {
            @Override
            public Boolean call(Rating rating) throws Exception {
                Double dRating = rating.rating();
                if (0 == dRating.compareTo(-1.0)) {//过滤只买1次的rating
                    return false;//去除
                }
                return true;
            }
        });

        long rdd_realRatingFiltered_count = rdd_realRatingFiltered.count();

        JavaRDD<Rating>[] rdd_realRatings = rdd_realRatingFiltered.randomSplit(new double[]{0.7, 0.3}, 111l);
        JavaRDD<Rating> rdd_train = rdd_realRatings[0];//.repartition(numPartitions);
        JavaRDD<Rating> rdd_test = rdd_realRatings[1];//.repartition(numPartitions);

        int rank = 30;//[10,50]
        int numIterations = 20;//[20]
        double lambda = 0.01;//[0.01,1.0]
        int alpha = 1;//[1,40]

//        for ()

        //使用ALS训练数据建立推荐模型 Build the recommendation model using ALS


        final MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(rdd_train), rank, numIterations, 1);

        JavaRDD<Tuple2<Object, Object>> rdd_testUserProducts = rdd_test.map(
                new Function<Rating, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(Rating r) {
                        return new Tuple2<Object, Object>(r.user(), r.product());
                    }
                }
        );

        //使用推荐模型对 rdd_testUserProducts 用户商品进行预测评分，得到预测评分的数据集
        JavaPairRDD<Tuple2<Integer, Integer>, Double> rdd_predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(rdd_testUserProducts)).toJavaRDD().map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
                                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                                        new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                            }
                        }
                ));

        //将真实评分数据集与预测评分数据集进行合并
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> rdd_testsAndPreds =
                JavaPairRDD.fromJavaRDD(rdd_test.map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
                                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                                        new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                            }
                        }
                )).join(rdd_predictions);
//        Long rl = new Random().nextLong();
//        rdd_testsAndPreds.repartition(1).saveAsTextFile("/dw_ext/outrealpre.data" + 332232);

        JavaRDD<Tuple2<Double, Double>> rdd_testsAndPredsValues = rdd_testsAndPreds.values();

        //然后计算均方差，注意这里没有调用 math.sqrt方法
        double MSE = JavaDoubleRDD.fromRDD(rdd_testsAndPredsValues.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        Double err = pair._1() - pair._2();
                        return err * err;
                    }
                }
        ).rdd()).mean();
        System.out.println(rank + " " + numIterations);
        System.out.println("rdd_realRating_count = " + rdd_realRating_count);
        System.out.println("rdd_realRatingFiltered_count = " + rdd_realRatingFiltered_count);
        System.out.println("Mean Squared Error = " + MSE);
    }
}