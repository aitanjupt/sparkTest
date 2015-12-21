package com.angel.hiveimpala;

import java.sql.*;

/**
 * Created by dell on 2015/10/16.
 */
public class HiveTask {
    private static final String SQL_STATEMENT = "SELECT systemid, businessid, businesstype, to_date (trade_time) statDate, COUNT(*) orderAmount FROM hive_hbase_orders WHERE `status` IN ( 'SUCCESS', 'REFUND', 'PARTIAL_REFUND' ) AND to_date (trade_time) BETWEEN '2015-01-01' AND '2015-10-15' GROUP BY systemid, businessid, businesstype, to_date (trade_time)";
    private static final String HiveD_HOST = "192.168.181.190";
    private static final String HIVE_JDBC_PORT = "10000";
    private static final String CONNECTION_URL = "jdbc:hive2://" + HiveD_HOST + ':' + HIVE_JDBC_PORT + "/default";

    private static final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

    public static void main(String[] args) {
        System.out.println("\n=============================================");
        System.out.println("Cloudera Hive JDBC Example");
        System.out.println("Using Connection URL: " + CONNECTION_URL);
        System.out.println("Running Query: " + SQL_STATEMENT);
        Connection con = null;
        try {
            Class.forName(JDBC_DRIVER_NAME);
            con = DriverManager.getConnection(CONNECTION_URL,"hdfs","12x3d456");
            Statement stmt = con.createStatement();
            ResultSet rs = stmt.executeQuery(SQL_STATEMENT);
            System.out.println("\n== Begin Query Results ======================");
            // print the results to the console
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
            System.out.println("== End Query Results =======================\n\n");
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                con.close();
            } catch (Exception e) {
            }
        }
    }

}