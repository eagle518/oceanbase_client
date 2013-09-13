package com.alipay.oceanbase;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

public class Test1 {

    public static final int DEFAULT_INITIAL_SIZE = 0;
    public static final int DEFAULT_MAX_ACTIVE   = 8;
    public static final int DEFAULT_MAX_IDLE     = 8;
    public static final int DEFAULT_MAX_WAIT     = 1000;

    public static void main(String[] args) throws Exception {

        /* Connection conn = DriverManager
             .getConnection(
                 "jdbc:mysql://10.232.36.32:48947?useLocalSessionState=true&useLocalTransactionState=true",
                 "admin", "admin");

         conn.setAutoCommit(false);

         PreparedStatement pstmt = conn.prepareStatement("insert into t1 values (1, 1)");
         int updateCount = pstmt.executeUpdate();
         System.out.println(updateCount);

         conn.rollback();

         pstmt.close();
         conn.close();*/

        /*if (pstmt.execute()) {
            ResultSet rs = pstmt.getResultSet();
            System.out.println("11");
            while (rs.next()) {
                System.out.println("22");
                System.out.println(rs.getString(1));
            }
        }*/

        OceanbaseDataSourceProxy oceanbaseDataSourceProxy = new OceanbaseDataSourceProxy();

        oceanbaseDataSourceProxy
            .setConfigURL("http://obconsole.test.alibaba-inc.com/ob-config/config.co?dataId=test_junbian");
        oceanbaseDataSourceProxy.setPeriod(100);
        oceanbaseDataSourceProxy.setMinIdle(4);
        oceanbaseDataSourceProxy.setMaxActive(6);
        oceanbaseDataSourceProxy.setInitialSize(0);
        oceanbaseDataSourceProxy.setQueryTimeout(1);
        oceanbaseDataSourceProxy.init();

        Connection conn = oceanbaseDataSourceProxy.getConnection();
        /*int max = 50000;
        StringBuilder sb = new StringBuilder("replace into t1 values ");
        for (int i = 0; i < max; i++) {
            sb.append("(").append(i).append(",").append(i).append(")");
            if (i != max - 1) {
                sb.append(",");
            }
        }*/

        Statement stmt = conn.createStatement();
        boolean result = stmt.execute("SET NAMES latin1");
        
        System.out.println(result);
//        while (rs.next()) {
//            System.out.println(rs.getString(1));
//        }

        /*int replaceCount = stmt.executeUpdate(sb.toString());
        System.out.println(replaceCount);*/

        //        PreparedStatement pstmt = conn.prepareStatement(sb.toString());
        //        System.out.println(pstmt.executeUpdate());
        //        ResultSet rs = pstmt.executeQuery();
        //        Statement stmt = conn.createStatement();
        //        boolean result = stmt.execute("desc t1");
        //        if (result) {
        //        ResultSet rs = stmt.getResultSet();
        /*while (rs.next()) {
            System.out.print(rs.getString(1));
            System.out.print("|");
            System.out.print(rs.getString(2));
            System.out.print("|");
            System.out.print(rs.getString(3));
            System.out.print("|");
            System.out.print(rs.getString(4));
            System.out.println("|");
        }*/
        //        }
        //        conn.close();

        /*for (int i = 4000; i < 4100; i++) {
            try {
                 Properties p = new Properties();
                 p.put("useLocalSessionState", "true");
                 p.put("useLocalTransactionState", "true");
                 p.put("user", "admin");
                 p.put("password", "admin");
                 Connection conn = DriverManager.getConnection("jdbc:mysql://10.232.36.32:48947", p);

                Connection conn = oceanbaseDataSourceProxy.getConnection();
                Statement stmt = conn.createStatement();
                stmt.execute("insert into t1(c1,c2) values(" + i + "," + i + ")");
                conn.close();

                conn = oceanbaseDataSourceProxy.getConnection();
                stmt = conn.createStatement();
                stmt.execute("select * from t1 where c1 = 1");
                conn.close();

                conn = oceanbaseDataSourceProxy.getConnection();
                stmt = conn.createStatement();
                stmt.execute("select * from t1 ");
                conn.close();

            } catch (SQLException sqlException) {
                throw sqlException;
            }

            TimeUnit.SECONDS.sleep(1L);
        }*/

        /*for (;;) {
            Connection conn = oceanbaseDataSourceProxy.getConnection();
            Statement stmt = conn.createStatement();
            stmt.execute("insert into t1 values('a',1)");
            boolean result= stmt.execute("select /*+read_cluster(slave)/ * from __all_cluster");
            /*PreparedStatement pstmt = conn
                .prepareStatement("select /*+read_cluster(slave)/ * from __all_cluster");
            boolean result = pstmt.execute();/
            System.out.println(result);
            //        boolean result = stmt.execute("select * from __all_cluster");
            ResultSet rs = null;
            if (result) {
                rs = pstmt.getResultSet();
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                }
            }
        }*/

        /*    PreparedStatement pstmt = conn
               .prepareStatement("select sum(total_amount) total_amount,sum(total_count) total_count,max(gmt_pay_latest) gmt_pay_latest, min(gmt_pay_earliest) gmt_pay_earliest from biz_action_daily where biz_platform_type = ? and user_id = ? and ((stat_time >= ? and stat_time < ?))");
           pstmt.setString(1, "ACQUIRING");
           pstmt.setString(2, "2088002010549272");
           pstmt.setDate(3, new Date(new java.util.Date().getTime()));
           pstmt.setDate(4, new Date(new java.util.Date().getTime()));

           ResultSet rs = pstmt.executeQuery();
           while (rs.next()) {
               System.out.println(rs.getString(1));
           }*/
        /*Connection conn = oceanbaseDataSourceProxy.getConnection();
        Statement stmt = conn.createStatement();
        System.out.println(stmt.executeUpdate("create table t5(c1 int primary key, c2 int)"));*/

        /*for (;;) {

            try {
                java.sql.PreparedStatement pstmt = conn
                    .prepareStatement("insert into ipda_auction_competitor (seller_id, competitor_id, auction_ids) values (?,?,?)");

                pstmt.setInt(1, 1);
                pstmt.setInt(2, 10000);
                pstmt.setString(3, "中国");
                pstmt.executeUpdate();

                pstmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                System.err.println("error");
            }

        }*/

        //        conn.close();
        //        for (int i = 0; i < 8; i++) {
        //            Thread d = new Thread(new Task(oceanbaseDataSourceProxy));
        //            d.start();
        //        }
    }
}

class Task implements Runnable {

    private DataSource ds;

    public Task(DataSource ds) {
        this.ds = ds;
    }

    @Override
    public void run() {
        for (;;) {
            try {
                Connection conn = ds.getConnection();
                java.sql.Statement pstmt = conn.createStatement();

                ResultSet rs = pstmt.executeQuery("select * from __all_client");
                while (rs.next()) {
                    System.out.println(rs.getString(1));
                }
                rs.close();
                pstmt.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

}
