package com.timliu.spark.java;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 *
 */
public class TestSparkSQLThriftServerJava {
	public static void main(String[] args) throws  Exception{
		Class.forName("org.apache.hive.jdbc.HiveDriver");
//		Connection conn = DriverManager.getConnection("jdbc:hive2://s201:10000/applogsdb","","");
		Connection conn = DriverManager.getConnection("jdbc:hive2://192.168.11.70:10000/applogsdb","","");
		PreparedStatement ppst = conn.prepareStatement("select * from ext_startup_logs");


		ResultSet rs = ppst.executeQuery();
		while(rs.next()){
			String appid = rs.getString("appid");
			System.out.println(appid);
		}
		rs.close();
		conn.close();
	}
}
