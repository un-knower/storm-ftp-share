package com.bonc.storm.hdfs2ftp;

import net.sf.json.JSONObject;

public class TestJSON {

	public static void testJSON(String json) {
		JSONObject jsonObj = JSONObject.fromObject(json);
		String filename = (String) jsonObj.get("file_name");
		String filepath = jsonObj.getString("file_path");
		System.out.println(filename+"  :  "+filepath);
	}
	
	public static void main(String[] args) {
		String json = "{\"server_address\":\"192.168.8.53\",\"operate_type\":\"1\",\"begin_time\":\"2015-12-09 15:05:19\",\"file_size\":\"5890090\",\"client_address\":\"192.168.8.51\",\"end_time\":\"2015-12-09 15:05:19\",\"ftp_account\":\"vascilpf\",\"file_path\":\"/itf/vasc/dpi/qixin/chongqing/03_102_0_00_20151209150511_27925.txt\",\"file_name\":\"03_102_0_00_20151209150511_27925.txt\",\"version\":\"V1.0.6\"}";
//		System.out.println(testJSON(json));
		testJSON(json);
	}
	
}
