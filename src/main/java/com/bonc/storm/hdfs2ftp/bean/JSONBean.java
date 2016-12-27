package com.bonc.storm.hdfs2ftp.bean;

import net.sf.json.JSONObject;

/**
 * 对JSONObject的封装
 * @author yinglin
 *
 */
public class JSONBean {
	
	private JSONObject jsonObj;
	
	public JSONBean(JSONObject obj){
		this.jsonObj = obj;
	}
	
	
	public String getString(String name){
		
		if(name != null){
			
			try {
				return this.jsonObj.getString(name);
			} catch (Exception e) {
				e.printStackTrace();
				return null;
			}
		}
		return null;
	}


	public double getDouble(String name) {
		
		if(name != null){
			
			try {
				return this.jsonObj.getDouble(name);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return 0;
	}


	public int getInt(String name) {
		if(name != null){
			
			try {
				return this.jsonObj.getInt(name);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		
		return 0;
	}
	
	
}
