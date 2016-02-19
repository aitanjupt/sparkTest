package com.angel.mlib;


import com.angel.util.SparkUtil;
import org.ansj.domain.Term;
import org.ansj.splitWord.analysis.ToAnalysis;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.mllib.clustering.DistributedLDAModel;
import org.apache.spark.mllib.clustering.LDA;
import org.apache.spark.mllib.feature.HashingTF;
import org.apache.spark.mllib.feature.IDF;
import org.apache.spark.mllib.feature.IDFModel;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.linalg.Matrix;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;
import scala.Tuple3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/*
spark-submit \
--master yarn-client --class com.angel.mlib.TFIDF_LDATest \
--files hive-site.xml \
--driver-class-path lib/spark-assembly-1.6.0-hadoop2.2.0.jar \
--jars lib/hbase-client-0.98.6-cdh5.3.6.jar,lib/hbase-common-0.98.6-cdh5.3.6.jar\
,lib/hbase-protocol-0.98.6-cdh5.3.6.jar,lib/hbase-server-0.98.6-cdh5.3.6.jar\
,lib/htrace-core-2.04.jar,lib/zookeeper.jar\
,lib/ansj_seg-3.3.jar,lib/nlp-lang-1.1.jar \
spark-test-1.0.jar
 */
public class TFIDF_LDATest implements Serializable {

    static final HashingTF hashingTF = new HashingTF();

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("TFIDF_LDATest");
        JavaSparkContext sc = new JavaSparkContext(conf);
        // Load and parse the data
        String path = "/dw_ext/mllib/LDA.test";
        JavaRDD<String> text = sc.textFile(path);

        JavaRDD<Iterable<String>> documentsParsed = getFenCi(text);

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

        wordListRDD.repartition(1).saveAsTextFile("/dw_ext/mllib/LDATestWordList.out");

        JavaRDD<Vector> parsedData = getTFIDF(documentsParsed);//某一行的单词的Vector

        // Index documents with unique IDs
        JavaPairRDD<Long, Vector> corpus = JavaPairRDD.fromJavaRDD(parsedData.zipWithIndex().map(
                new Function<Tuple2<Vector, Long>, Tuple2<Long, Vector>>() {
                    public Tuple2<Long, Vector> call(Tuple2<Vector, Long> doc_id) {
                        return doc_id.swap();
                    }
                }
        ));
        corpus.cache();

        // Cluster the documents into three topics using LDA
        DistributedLDAModel ldaModel = (DistributedLDAModel) new LDA().setK(6).run(corpus);


//        JavaRDD<Tuple3<Long, int[], double[]>> dd = ldaModel.javaTopTopicsPerDocument(4);

//        JavaRDD<String> dds = dd.map(new Function<Tuple3<Long, int[], double[]>, String>() {
//            @Override
//            public String call(Tuple3<Long, int[], double[]> longTuple3) throws Exception {
//                StringBuilder sb = new StringBuilder();
//                sb.append(longTuple3._1()).append(": ");
//                for (int i : longTuple3._2()) {
//                    sb.append(i).append(" ");
//                }
//                sb.append(" ; ");
//                for (double d : longTuple3._3()) {
//                    sb.append(d).append(" ");
//                }
//                return sb.toString();
//            }
//        });

//        dds.repartition(1).saveAsTextFile("/dw_ext/mllib/TFIDF_LDATest.out");

//        Tuple2<int[], double[]>[] topics = ldaModel.describeTopics(5);
//        for (Tuple2<int[], double[]> tuple2 : topics) {
//            for (int l : tuple2._1) {
//                System.out.print(l + " ");
//            }
//            System.out.print(" ; ");
//            for (double d : tuple2._2) {
//                System.out.print(d + " ");
//            }
//            System.out.println();
//        }


//        Tuple2<long[], double[]>[] dds2 = ldaModel.topDocumentsPerTopic(10);


        JavaRDD<Tuple3<Long, int[], double[]>> topicDocument = ldaModel.javaTopTopicsPerDocument(3);

        for (Tuple3<Long, int[], double[]> tuple3 : topicDocument.collect()) {
            System.out.print(tuple3._1() + " : ");
            for (int i : tuple3._2()) {
                System.out.print(i + " ");
            }
            System.out.print(" ; ");
            for (double d : tuple3._3()) {
                System.out.print(d + " ");
            }
            System.out.println();
        }

//        SparkUtil.print(d.collect());

        // Output topics. Each is a distribution over words (matching word count vectors)
        System.out.println("Learned topics (as distributions over vocab of " + ldaModel.vocabSize()
                + " words):");
//        Matrix topics = ldaModel.topicsMatrix();
//        for (int topic = 0; topic < 3; topic++) {
//            System.out.print("Topic " + topic + ":");
//
//            for (int word = 0; word < ldaModel.vocabSize(); word++) {
//                System.out.print(" " + topics.apply(word, topic));
//            }
//            System.out.println();
//        }

    }

    private static JavaRDD<Iterable<String>> getFenCi(JavaRDD<String> data) {
        JavaRDD<Iterable<String>> documentsParsed = data.map(new Function<String, Iterable<String>>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                List<Term> terms = ToAnalysis.parse(s);
                List<String> array = new ArrayList<>();
                for (Term term : terms) {
                    String t = term.getName().trim();
                    String natureStr = term.getNatureStr();
                    if ((!t.isEmpty()) && (natureStr.equals("n"))) {
                        array.add(t);
                    }
                }
                return array;
            }
        });
        return documentsParsed;
    }


    private static JavaRDD<Vector> getTFIDF(JavaRDD<Iterable<String>> documentsParsed) {

        JavaRDD<Vector> tf = hashingTF.transform(documentsParsed);//算出词频

        IDFModel idf = new IDF().fit(tf);
        JavaRDD<Vector> tfidf = idf.transform(tf);//使用IDF加权

        return tfidf;
    }
}