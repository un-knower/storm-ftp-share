package com.bonc.storm.hdfs2ftp.util;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {

	
	/**
	 * 获取当前系统时间
	 * @param format
	 * @return
	 */
	public static String getNowTime(String format) {
		//设置日期格式
		SimpleDateFormat df = new SimpleDateFormat(format);
		Date date = new Date();
		return df.format(date);
	}
	
	public static void main(String[] args) {
		System.out.println(getNowTime("yyyy-MM-dd HH:mm:ss S"));
		System.out.println(getNowTime("yyyyMMddHHmmssS"));
	}
	
}
