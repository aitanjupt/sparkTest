package com.angel.mlib;

import com.esotericsoftware.kryo.Kryo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.hbase.util.Base64;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.serializer.KryoRegistrator;
import scala.Tuple2;

import java.io.IOException;
import java.io.Serializable;
import java.util.*;

/**
 * Created by dell on 2015/12/11.
 */
public class ALSTrnPrd implements KryoRegistrator {


    public static void main(String[] args) {
        System.setProperty("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        System.setProperty("spark.kryo.registrator", "com.angel.mlib.ALSTrnPrd");


        SparkConf conf = new SparkConf().setAppName("Collaborative Filtering ALS");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load and parse the train data from hive hive文件非压缩格式rcfile
        String trainPath = "/dw_ext/sinaad/dm/mds/mds_member_item_num/dt=20151218/000000_0";
        JavaRDD<String> trainData = sc.textFile(trainPath);
        JavaRDD<Rating> trainRatings = trainData.map(
                new Function<String, Rating>() {
                    public Rating call(String s) {
                        String[] sarray = s.split("\t");
                        return new Rating(Integer.parseInt(sarray[0]), Integer.parseInt(sarray[1]),
                                Double.parseDouble(sarray[3]));
                    }
                }
        );
        JavaRDD<Integer> users = trainRatings.map(new Function<Rating, Integer>() {
            @Override
            public Integer call(Rating rating) throws Exception {
                Integer user = rating.user();
                return user;
            }
        }).distinct();

        //使用ALS训练数据建立推荐模型 Build the recommendation model using ALS
        int rank = 10;
        int numIterations = 20;
        final MatrixFactorizationModel model = ALS.train(JavaRDD.toRDD(trainRatings), rank, numIterations, 0.01);

        List<Integer> mids = users.collect();
        List<Tuple2> ratings = new ArrayList<>();
        for (Integer mid : mids) {
            Rating[] rating = model.recommendProducts(mid, 10);
            Map<Integer, Double> uRating = new HashMap<>();
            for (int i = 0; i < rating.length; i++) {
                uRating.put(rating[i].product(), rating[i].rating());
            }
            Tuple2<Integer, Map<Integer, Double>> uRattings = new Tuple2<>(mid, uRating);
            ratings.add(uRattings);
        }

        JavaRDD<Tuple2> tRatings = sc.parallelize(ratings);
        Long rl = new Random().nextLong();
        tRatings.repartition(1).saveAsTextFile("/dw_ext/outuser10item.data" + rl);

    }

//    void bad5(JavaRDD<Integer> users, final MatrixFactorizationModel model){
//        JavaRDD<Rating> prdictions = users.flatMap(new FlatMapFunction<Integer, Rating>() {
//            @Override
//            public Iterable<Rating> call(Integer integer) throws Exception {
//                Rating[] ratings = model.recommendProducts(integer, 10);
//                return Arrays.asList(ratings);
//            }
//        });
//
//        prdictions.repartition(1).saveAsTextFile("file:///home/mllib/outall.data");
//    }

//    void bad4hbase() {
    //        Configuration hbconf = HBaseConfiguration.create();
//        hbconf.set(TableInputFormat.INPUT_TABLE, "member");
//        Scan scan = new Scan();
//        scan.addFamily(Bytes.toBytes("xkeshi"));
//        scan.addColumn(Bytes.toBytes("xkeshi"), Bytes.toBytes("id"));
//        ClientProtos.Scan proto = null;
//        try {
//            proto = ProtobufUtil.toScan(scan);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//        String ScanToString = Base64.encodeBytes(proto.toByteArray());
//        conf.set(TableInputFormat.SCAN, ScanToString);
//        JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD = sc.newAPIHadoopRDD(hbconf, TableInputFormat.class, ImmutableBytesWritable.class, Result.class);
//        List<Result> results = hBaseRDD.values().collect();

//        for (Result result : results)
//        {
////            byte[] byte_id = result.getValue(Bytes.toBytes("xkeshi"), Bytes.toBytes("id"));
////            Integer memberId = Integer.valueOf(Bytes.toString(byte_id));
//            Integer memberId = 54600;
//            System.out.println("memberId=" + memberId);
//            Rating[] rcmRatting = null;
//            try {
//                rcmRatting = model.recommendProducts(memberId, 10);
//            } catch (Exception ex) {
//                System.out.println("memberId=" + memberId + " " + ex);
//            }
//            if (null != rcmRatting) {
//                HashMap<Integer, Double> rcmPR = new HashMap<>();
//                for (Rating r : rcmRatting) {
//                    rcmPR.put(r.product(), r.rating());
//                    System.out.println(memberId + " " + r.product() + " " + r.rating());
//                }
//            }
//        }
//    }

//    void bad3() {
    //        Long count = hBaseRDD.count();
//        List<Tuple2<ImmutableBytesWritable, Result>> tuples = hBaseRDD.take(count.intValue());
//        for (Tuple2<ImmutableBytesWritable, Result> tuple : tuples){
//            byte[] byte_id = tuple._2().getValue(Bytes.toBytes("xkeshi"), Bytes.toBytes("id"));
//            Integer memberId = Integer.valueOf(Bytes.toString(byte_id));
//            Rating[] rcmRatting = model.recommendProducts(memberId, 10);
//            HashMap<Integer, Double> rcmPR = new HashMap<>();
//            for (Rating r : rcmRatting) {
//                rcmPR.put(r.product(), r.rating());
//                System.out.println(memberId + " " + r.product() + " " + r.rating());
//            }
//        }
//    }

//    void bad2(){
//        hBaseRDD.foreachPartition(new VoidFunction<Iterator<Tuple2<ImmutableBytesWritable, Result>>>() {
//            @Override
//            public void call(Iterator<Tuple2<ImmutableBytesWritable, Result>> tuple2Iterator) throws Exception {
//                while (tuple2Iterator.hasNext()) {
//                    Tuple2<ImmutableBytesWritable, Result> tuple = tuple2Iterator.next();
//                    byte[] byte_id = tuple._2().getValue(Bytes.toBytes("xkeshi"), Bytes.toBytes("id"));
//                    Integer memberId = Integer.valueOf(Bytes.toString(byte_id));
//                    Rating[] rcmRatting = model.recommendProducts(memberId, 10);
//                    HashMap<Integer, Double> rcmPR = new HashMap<>();
//                    for (Rating r : rcmRatting) {
//                        rcmPR.put(r.product(), r.rating());
//                        System.out.println(memberId + " " + r.product() + " " + r.rating());
//                    }
//                }
//            }
//        });
//    }

//    private static void bad(JavaPairRDD<ImmutableBytesWritable, Result> hBaseRDD){
//        final JavaRDD<Tuple2<Integer, HashMap<Integer, Double>>> rcm = hBaseRDD.map(
//                new Function<Tuple2<ImmutableBytesWritable, Result>, Tuple2<Integer, HashMap<Integer, Double>>>() {
//                    @Override
//                    public Tuple2<Integer, HashMap<Integer, Double>> call(Tuple2<ImmutableBytesWritable, Result> hbresult) throws Exception {
//                        byte[] byte_id = hbresult._2().getValue(Bytes.toBytes("xkeshi"), Bytes.toBytes("id"));
//                        Integer memberId = Integer.valueOf(Bytes.toString(byte_id));
////                        Rating[] rcmRatting = model.recommendProducts(memberId, 10);
////                        if (null == rcmRatting) {
////                            return null;
////                        }
//                        Rating[] rcmRatting = model.recommendProducts(memberId, 10);
//                        HashMap<Integer, Double> rcmPR = new HashMap<>();
//                        for (Rating r : rcmRatting) {
//                            rcmPR.put(r.product(), r.rating());
//                            System.out.println(memberId + " " + r.product() + " " + r.rating());
//                        }
//                        Tuple2<Integer, HashMap<Integer, Double>> ret = new Tuple2<>(memberId, rcmPR);
//                        return ret;
//                    }
//                }
//        );
//        rcm.repartition(1).saveAsTextFile("file:///home/mllib/outall.data");
//    }

    @Override
    public void registerClasses(Kryo kryo) {
//        kryo.register(Result.class);
    }
}