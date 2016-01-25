package com.angel.mlib;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import scala.Tuple2;

import java.io.Serializable;

/***
 * 测试是否可以跨区域推荐
 * 用户1与其他用户的商品不在一个区域内
 */
public class LDATest implements Serializable {

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

//    private static void test(){
//        SparkConf conf = new SparkConf().setAppName("LDA Example");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//
//        // Load and parse the data
//        String path = "data/mllib/sample_lda_data.txt";
//        JavaRDD<String> data = sc.textFile(path);
//        JavaRDD<Vector> parsedData = data.map(
//                new Function<String, Vector>() {
//                    public Vector call(String s) {
//                        String[] sarray = s.trim().split(" ");
//                        double[] values = new double[sarray.length];
//                        for (int i = 0; i < sarray.length; i++)
//                            values[i] = Double.parseDouble(sarray[i]);
//                        return Vectors.dense(values);
//                    }
//                }
//        );
//        // Index documents with unique IDs
//        JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(
//                new Function<Tuple2<Vector, Long>, Tuple2<Long, Vector>>() {
//                    public Tuple2<Long, Vector> call(Tuple2<Vector, Long> doc_id) {
//                        return doc_id.swap();
//                    }
//                }
//        ));
//        corpus.cache();
//
//        // Cluster the documents into three topics using LDA
//        DistributedLDAModel ldaModel = new LDA().setK(3).run(corpus);
//
//        // Output topics. Each is a distribution over words (matching word count vectors)
//        System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
//                + " words):");
//        Matrix topics = ldaModel.topicsMatrix();
//        for (int topic = 0; topic < 3; topic++) {
//            System.out.print("Topic " + topic + ":");
//            for (int word = 0; word < ldaModel.vocabSize(); word++) {
//                System.out.print(" " + topics.apply(word, topic));
//            }
//            System.out.println();
//        }
//
//        ldaModel.save(sc.sc(), "myLDAModel");
//        DistributedLDAModel sameModel = DistributedLDAModel.load(sc.sc(), "myLDAModel");
//    }
}