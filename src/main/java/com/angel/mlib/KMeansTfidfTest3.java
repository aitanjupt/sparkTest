package com.angel.mlib;


import com.angel.util.SparkUtil;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;


/*
spark-submit \
--master yarn-client --class com.angel.mlib.KMeansTfidfTest3 \
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
public class KMeansTfidfTest3 implements Serializable {

    //方案一KMeansTfidfTest：方案一热销商品有噪音，（卖的好的有这些词）
    // step1:所有商品销售统计；step2:对统计数据Kmean算出热销商品；step3:对热销商品名称作TF挖掘

    //方案二KMeansTfidfTest2：（这些词卖的好）
    // step1:对所有销售订单内的商品名称作TFIDF挖掘；step2:对挖掘结果Kmean算出热点词

    //方案三KMeansTfidfTest3：
    // step1:对所有销售订单内的商品ID作TFIDF挖掘（消除噪音）；step2:对上步结果作Kmean得出热销商品ID；step3:对热销商品名称作TF挖掘
    static SparkConf conf = new SparkConf().setAppName("KMeansTfidfTest");
    static final transient JavaSparkContext sc = new JavaSparkContext(conf);

    public static void main(String[] args) {

        JavaRDD<String> read = sc.textFile("/dw_ext/mllib/kmean-tfidf.order3.test");

        JavaRDD<Iterable<Integer>> dataTrain = read.map(new Function<String, Iterable<Integer>>() {
            @Override
            public Iterable<Integer> call(String s) throws Exception {
                List<Integer> ret = new ArrayList<>();
                String[] ss = s.split(",");
                for (String tmp : ss) {
                    Integer i = Integer.parseInt(tmp);
                    ret.add(i);
                }
                return ret;
            }
        });

        JavaRDD<Integer> dataFlat = dataTrain.flatMap(new FlatMapFunction<Iterable<Integer>, Integer>() {
            @Override
            public Iterable<Integer> call(Iterable<Integer> integers) throws Exception {
                List<Integer> ret = new ArrayList<Integer>();
                for (Integer i : integers) {
                    ret.add(i);
                }
                return ret;
            }
        });

        JavaRDD<Iterable<Integer>> dataGroup = dataFlat.groupBy(new Function<Integer, Object>() {
            @Override
            public Object call(Integer integer) throws Exception {
                return 1;
            }
        }).values();

//        SparkUtil.print(dataGroup.collect());
        doTFIDF(dataTrain, dataGroup);
    }


    private static void doTFIDF(JavaRDD<Iterable<Integer>> dataTrain, JavaRDD<Iterable<Integer>> dataGroup) {
        final HashingTF hashingTF = new HashingTF();

        JavaRDD<Vector> tfTrain = hashingTF.transform(dataTrain);//算出训练词频
        IDFModel idf = new IDF().fit(tfTrain);//训练出IDF矩阵

        JavaRDD<Vector> tfGroup = hashingTF.transform(dataGroup);//算出真实词频
        JavaRDD<Vector> tfidf = idf.transform(tfGroup);//对真实词频使用IDF加权

//        JavaPairRDD<Integer, Double> tfidfFlat = tfidf.flatMapToPair(new PairFlatMapFunction<Vector, Integer, Double>() {
//            @Override
//            public Iterable<Tuple2<Integer, Double>> call(Vector vector) throws Exception {
//                List<Tuple2<Integer, Double>> ret = new ArrayList<>();
//                int[] indices = vector.toSparse().indices();
//                for (int index : indices) {
//                    Double value = vector.apply(index);
//                    Tuple2<Integer, Double> t = new Tuple2<>(index, value);
//                    ret.add(t);
//                }
//                return ret;
//            }
//        }).distinct();


//        print(wordListRDD.collect());
//        print(tf.collect());
//        SparkUtil.print(tf.collect());
//        SparkUtil.print(tfidf.collect());
//        SparkUtil.print(tfidfFlat.collect());
        tfGroup.repartition(1).saveAsTextFile("/dw_ext/mllib/tf8.out");
        tfidf.repartition(1).saveAsTextFile("/dw_ext/mllib/tfidf8.out");
    }

}

