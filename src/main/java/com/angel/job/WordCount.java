package com.angel.job;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Created by dell on 2015/8/11.
 */
public class WordCount {

    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

//        JavaSparkContext ctx = new JavaSparkContext("spark://192.168.181.170:7077", "WordCount",
//                "/opt/cloudera/parcels/CDH/lib/spark/bin",
//                "D:\\Program Files\\repository\\com\\angel\\spark-test\\1.0\\spark-test-1.0.jar");
//        JavaRDD<String> lines = ctx.textFile("D:/tmp/supervisord.log", 1);

        SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> lines = ctx.textFile("file:///opt/cloudera/parcels/CDH/lib/spark/bin/load-spark-env.sh", 1);

        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {   //flatMap
            public Iterable<String> call(String s) {    //FlatMapFunction<String, String>
                return Arrays.asList(SPACE.split(s));
            }
        });

        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {   //PairFunction<String, String, Integer>����String��<String, Integer>
            public Tuple2<String, Integer> call(String s) {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() { //Function2<Integer, Integer, Integer>����<Integer, Integer>��Integer
            public Integer call(Integer i1, Integer i2) {
                return i1 + i2;
            }
        });

        List<Tuple2<String, Integer>> output = counts.collect();    //
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        System.out.println("====:" + counts.count());

        System.exit(0);


//        ctx.stop();
    }
}
