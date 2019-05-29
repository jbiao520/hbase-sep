package com.javaoom.loquat.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.LinkedList;

public class HBaseConnectionPool {
    private static LinkedList<Connection> pool = new LinkedList<>();
    private static Configuration config = HBaseConfiguration.create();
    private static int Threshold = 10;
    static{
        String hbaseSiteXml = HBaseUtil.class
                .getClassLoader()
                .getResource("hbase-site.xml")
                .getPath();
        config.addResource(new Path(hbaseSiteXml));
    }

    public static Connection getConn(){
        if(pool.size()>0){
            return pool.removeFirst();
        }else{
            return newConnection();
        }
    }

    private static Connection newConnection(){
        Connection connection = null;
        try {
            connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return connection;
    }

    public static void close(Connection conn){
        if(pool.size()>=Threshold){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }else{
            pool.addLast(conn);
        }
    }
}
