package com.bonc.storm.hdfs2ftp.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.slf4j.LoggerFactory;

public class DBUtil {

	private static final org.slf4j.Logger logger = LoggerFactory.getLogger(DBUtil.class);
	private static String driverName;
	private static String url;
	private static String user;
	private static String password;
	
	public DBUtil(Configure configure) {
		try {
			// 从配置文件中获取数据库连接驱动
			driverName = configure.getString("driverClassName");
			// 从配置文件中获取数据库连接地址
			url = configure.getString("url");
			// 从配置文件中获取数据库用户名
			user = configure.getString("username");
			// 从配置文件中获取数据库用户名密码
			password = configure.getString("password");
			logger.info("数据库连接驱动：{}, 连接地址：{}, 数据库用户名：{}, 数据库密码：{} ", driverName, url, user, password);
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			logger.error("请检查连接驱动 {} 是否正确或者相应jar包是否存在", driverName, e);
		}
	}

	// 获得Connection对象所消耗资源会占到整个jdbc操作的85%以上
	// 批处理除外
	// 尽量减少获得Connection对象
	public Connection getConnection() {
		try {
			return DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	public void close(Connection conn) {
		if (conn != null) {
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
//		Properties props = new Properties();
//		FileInputStream fis = new FileInputStream("d:/db.properties");
//		props.load(fis);
//		System.out.println(props.getProperty("driverClass"));
//		System.out.println(props.getProperty("user"));
//		System.out.println(getConnection());
	}
}
