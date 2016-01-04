package com.angel.hadoopspark;

import com.esotericsoftware.kryo.Kryo;
import org.apache.hadoop.hive.ql.io.RCFileInputFormat;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.serializer.KryoRegistrator;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

/**
 * Created by dell on 2015/12/22.
 */
public class SparkRCFileTest implements Serializable {

    public static void main(String[] args) {
        test1();
    }

    static void test1() {
        SparkConf conf = null;
//        try {
        conf = new SparkConf().setAppName("SparkHiveTest");
//                    .registerKryoClasses(new Class<?>[]{
//                            Class.forName("org.apache.hadoop.io.LongWritable"),
//                            Class.forName("org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable")
//                    });
//        } catch (ClassNotFoundException e) {
//            e.printStackTrace();
//        }
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaPairRDD<LongWritable, BytesRefArrayWritable> rowPairRDD = sc.hadoopFile("/dc_ext/xbd/dm/ods/ods_dm_orderitem/dt=20151227/000006_0", RCFileInputFormat.class, LongWritable.class, BytesRefArrayWritable.class);

        JavaRDD<String> rowRDD = rowPairRDD.map
                (new Function<Tuple2<LongWritable, BytesRefArrayWritable>, String>() {
                     @Override
                     public String call(Tuple2<LongWritable, BytesRefArrayWritable> longWritableBytesRefArrayWritableTuple2) throws Exception {
                         BytesRefArrayWritable braw = longWritableBytesRefArrayWritableTuple2._2;
                         Text txt = new Text();
                         StringBuffer sb = new StringBuffer();
                         for (int i = 0; i < braw.size(); i++) {
                             BytesRefWritable v = braw.get(i);
                             txt.set(v.getData(), v.getStart(), v.getLength());
                             if (i == braw.size() - 1) {
                                 sb.append(txt.toString());
                             } else {
                                 sb.append(txt.toString() + "\t");
                             }
                         }
                         return sb.toString();
                     }
                 }
                );

        List<String> rows = rowRDD.collect();

        for (String row : rows) {
            System.out.println(row);
        }

        System.out.println("angel out" + rows.size());


    }

//    void bad(JavaPairRDD<LongWritable, BytesRefArrayWritable> rowPairRDD) {
//        List<Tuple2<LongWritable, BytesRefArrayWritable>> pairs = rowPairRDD.collect();//LongWritable报错serializable
//
//        for (Tuple2<LongWritable, BytesRefArrayWritable> pair : pairs) {
//            System.out.println(pair._1());
//        }
//
//        System.out.println("trainRatings.count: " + pairs.size());
//    }


}

//spark-submit --master spark://node190:7077 --jars lib/hive-exec-0.13.1-cdh5.3.6.jar,lib/hive-serde-0.13.1-cdh5.3.6.jar --class com.angel.hadoopspark.SparkRCFileTest spark-test-1.0.jarspark-submit --master yarn-client --jars lib/hive-exec-0.13.1-cdh5.3.6.jar,lib/hive-serde-0.13.1a.jar --class com.angel.hadoopspark.SparkRCFileTest spark-test-1.0.jar