package com.angel.mlib;


import com.angel.util.SparkUtil;
import com.google.common.base.*;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.IteratorUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import scala.*;
import scala.Function2;
import scala.runtime.BoxedUnit;

import java.io.Serializable;
import java.lang.Boolean;
import java.lang.Double;
import java.util.*;
import java.util.regex.Pattern;


/*
spark-submit \
--master yarn-client --class com.angel.mlib.KMeansTfidfTest2 \
--files hive-site.xml \
--driver-class-path ${jar_dir}/spark-assembly-1.6.0-hadoop2.2.0.jar\
:${jar_dir}/jdo-api-3.0.1.jar:${jar_dir}/derby-10.10.1.1.jar\
:${jar_dir}/datanucleus-api-jdo-3.2.6.jar:${jar_dir}/datanucleus-core-3.2.10.jar:${jar_dir}/datanucleus-rdbms-3.2.9.jar \
--jars ${jar_dir}/etl-util-1.0.jar,${jar_dir}/elasticsearch-1.7.1.jar,${jar_dir}/lucene-core-4.10.4.jar\
,${jar_dir}/hbase-client-0.98.6-cdh5.3.6.jar,${jar_dir}/hbase-common-0.98.6-cdh5.3.6.jar\
,${jar_dir}/hbase-protocol-0.98.6-cdh5.3.6.jar,${jar_dir}/hbase-server-0.98.6-cdh5.3.6.jar\
,${jar_dir}/htrace-core-2.04.jar,${jar_dir}/hive-serde-0.13.1-cdh5.3.6.jar\
,${jar_dir}/zookeeper.jar\
,${jar_dir}/ansj_seg-3.3.jar,${jar_dir}/nlp-lang-1.1.jar \
spark-test-1.0.jar
 */
/*
hadoop dfs -rm -r /dw_ext/mllib/kmean.test.out1
 */
public class KMeansTfidfTest2 implements Serializable {

    //方案一KMeansTfidfTest：方案一热销商品有噪音，（卖的好的有这些词）
    // step1:所有商品销售统计；step2:对统计数据Kmean算出热销商品；step3:对热销商品名称作TF挖掘

    //方案二KMeansTfidfTest2：（这些词卖的好）
    // step1:对所有销售订单内的商品名称作TFIDF挖掘；step2:对挖掘结果Kmean算出热点词

    //方案三KMeansTfidfTest3：
    // step1:对所有销售订单内的商品ID作TFIDF挖掘（消除噪音）；step2:对上步结果作Kmean得出热销商品ID；step3:对热销商品名称作TF挖掘
    static SparkConf conf = new SparkConf().setAppName("KMeansTfidfTest");
    static final transient JavaSparkContext sc = new JavaSparkContext(conf);

    public static void main(String[] args) {

        // Load and parse the data
        JavaRDD<String> data = sc.textFile("/dw_ext/mllib/kmean-tfidf.order2.test");
        doTFIDF(data);
    }


    private static void doTFIDF(JavaRDD<String> data) {
        final HashingTF hashingTF = new HashingTF();

        JavaRDD<Iterable<String>> documentsParsed = data.map(new Function<String, Iterable<String>>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                List<Term> terms = ToAnalysis.parse(s);
                List<String> array = new ArrayList<>();
                for (Term term : terms) {
                    String t = term.getName().trim();
                    if (!t.isEmpty()) {
                        array.add(t);
                    }
                }
                return array;
            }
        });

        JavaPairRDD<Integer, String> wordListRDD = documentsParsed.flatMapToPair(new PairFlatMapFunction<Iterable<String>, Integer, String>() {
            @Override
            public Iterable<Tuple2<Integer, String>> call(Iterable<String> strings) throws Exception {
                List<Tuple2<Integer, String>> ret = new ArrayList<>();
                for (String s : strings) {
                    int i = hashingTF.indexOf(s);
                    Tuple2<Integer, String> t = new Tuple2<>(i, s);
                    ret.add(t);
                }
                return ret;
            }
        }).distinct();

        JavaRDD<Vector> tf = hashingTF.transform(documentsParsed);//算出词频

        IDFModel idf = new IDF().fit(tf);
        JavaRDD<Vector> tfidf = idf.transform(tf);//使用IDF加权

        JavaPairRDD<Integer, Double> tfidfFlat = tfidf.flatMapToPair(new PairFlatMapFunction<Vector, Integer, Double>() {
            @Override
            public Iterable<Tuple2<Integer, Double>> call(Vector vector) throws Exception {
                List<Tuple2<Integer, Double>> ret = new ArrayList<>();
                int[] indices = vector.toSparse().indices();
                for (int index : indices) {
                    Double value = vector.apply(index);
                    Tuple2<Integer, Double> t = new Tuple2<>(index, value);
                    ret.add(t);
                }
                return ret;
            }
        }).distinct();

        JavaPairRDD<Integer, Tuple2<String, Double>> tfidfMsg = wordListRDD.join(tfidfFlat);

//        print(wordListRDD.collect());
//        print(tf.collect());
//        SparkUtil.print(tfidfMsg.collect());
        tfidfMsg.repartition(1).saveAsTextFile("/dw_ext/mllib/KMeansTfidfTest2.out");
    }

}

