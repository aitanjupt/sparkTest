package com.angel.hadoopspark;

/**
 * Created by dell on 2015/12/22.
 */
public class SparkHbaseTest {
    public static void main(String[] args) {
        //    void bad4hbase() {
        //        Configuration hbconf = HBaseConfiguration.create();
//        hbconf.set(TableInputFormat.INPUT_TABLE, "member");
//        Scan scan = new Scan();
//    scan.setStartRow(Bytes.toBytes("195861-1035177490"));
//    scan.setStopRow(Bytes.toBytes("195861-1072173147"));
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
    }
}
