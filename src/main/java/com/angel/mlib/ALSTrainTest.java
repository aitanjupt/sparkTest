package com.angel.mlib;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.io.Serializable;

/**
 * Created by dell on 2015/12/11.
 */
public class ALSTrainTest implements Serializable {

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Collaborative Filtering Example");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load and parse the train data
        String trainPath = "file:///home/mllib/2w.train";
        JavaRDD<String> trainData = sc.textFile(trainPath);
        JavaRDD<Rating> trainRatings = trainData.map(
                new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = s.split(",");
                        return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2]));
                    }
                }
        );

        // Load and parse the test data
        String testPath = "file:///home/mllib/4w.test";
        JavaRDD<String> testData = sc.textFile(testPath);
        JavaRDD<Rating> testRatings = testData.map(
                new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = s.split(",");
                        return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[2]));
                    }
                }
        );
        //从 testRating 中获得只包含用户和商品的数据集 Evaluate the model on train data
        JavaRDD<Tuple2<Object, Object>> testUserProducts = testRatings.map(
                new Function<Rating, Tuple2<Object, Object>>() {
                    public Tuple2<Object, Object> call(Rating r) {
                        return new Tuple2<Object, Object>(r.user(), r.product());
                    }
                }
        );

        //使用ALS训练数据建立推荐模型 Build the recommendation model using ALS
        int rank = 60;
        int numIterations = 20;
        MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainRatings), rank, numIterations);
//train
        //使用推荐模型对testUserProducts用户商品进行预测评分，得到预测评分的数据集
        JavaPairRDD<Tuple2<Integer, Integer>, Double> predictions = JavaPairRDD.fromJavaRDD(
                model.predict(JavaRDD.toRDD(testUserProducts)).toJavaRDD().map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
                                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                                        new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                            }
                        }
                ));

        //将真实评分数据集与预测评分数据集进行合并
        JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Double, Double>> testsAndPreds =
                JavaPairRDD.fromJavaRDD(testRatings.map(
                        new Function<Rating, Tuple2<Tuple2<Integer, Integer>, Double>>() {
                            public Tuple2<Tuple2<Integer, Integer>, Double> call(Rating r) {
                                return new Tuple2<Tuple2<Integer, Integer>, Double>(
                                        new Tuple2<Integer, Integer>(r.user(), r.product()), r.rating());
                            }
                        }
                )).join(predictions);
        testsAndPreds.repartition(1).saveAsTextFile("file:///home/mllib/testoutpredic2.data");

        JavaRDD<Tuple2<Double, Double>> testsAndPredsValues = testsAndPreds.values();

        //然后计算均方差，注意这里没有调用 math.sqrt方法
        double MSE = JavaDoubleRDD.fromRDD(testsAndPredsValues.map(
                new Function<Tuple2<Double, Double>, Object>() {
                    public Object call(Tuple2<Double, Double> pair) {
                        Double err = pair._1() - pair._2();
                        return err * err;
                    }
                }
        ).rdd()).mean();
        System.out.println("Mean Squared Error = " + MSE);
    }
}