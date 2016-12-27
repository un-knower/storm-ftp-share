package com.bonc.storm.hdfs2ftp.util;

public class StringUtil {

	/**
	 * 检查字符串是否符合 host1:ip,host2:ip 格式
	 * @param hostStr
	 * @return
	 */
	public static boolean checkHost(String hostStr){
		
		if(hostStr != null){
			String[] hosts = hostStr.split(",");
			if(hosts != null && hosts.length >0){
				for(String host : hosts){
					if(host.split(":") == null || host.split(":").length == 0){
						return false;
					}				
				}
				return true;
			}
		}
		return false;
	}
	
}
