package com.bonc.storm.hdfs2ftp;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class TestCHMap {

	public static void main(String[] args) {
		ConcurrentMap<String, String> map = new ConcurrentHashMap<String, String>();
		map.put("a", "a");
		map.put("b", "b");
		System.out.println(map.get("a"));
		System.out.println(map.get("a"));
	}
	
}
