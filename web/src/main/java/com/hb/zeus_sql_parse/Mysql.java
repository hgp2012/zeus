package com.hb.zeus_sql_parse;

import java.sql.Connection;  
import java.sql.DriverManager;  
import java.sql.PreparedStatement;  
import java.sql.SQLException;  
import java.sql.ResultSet;

public class Mysql {
	public static final String url = "jdbc:mysql://10.150.10.152/zeus_lky";  
    public static final String name = "com.mysql.jdbc.Driver";  
    public static final String userName = "root";  
    public static final String password = "Root_1q2w3e";  
	
//	public static final String url = "jdbc:mysql://192.168.13.55/zeus_lky";  
//    public static final String name = "com.mysql.jdbc.Driver";  
//    public static final String userName = "root";  
//    public static final String password = "Hb_123456";  
  
    public Connection conn = null; 
    public PreparedStatement pst = null; 
  
    /**
     * 打开数据库驱动连接
     */
    public static Connection getConnection(){
        try {
        	Class.forName(name);//指定连接类型  
            Connection conn = DriverManager.getConnection(url, userName, password);
            return conn;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
  
    public static void close(Connection conn,PreparedStatement stmt,ResultSet rs){
        if(rs!=null)
            try {
                rs.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
                throw new RuntimeException(e1);
            }
        if(stmt!=null){
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
        if(conn!=null){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
                throw new RuntimeException(e);
            }
        }
    }

}
