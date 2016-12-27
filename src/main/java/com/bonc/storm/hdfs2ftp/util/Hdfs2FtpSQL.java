package com.bonc.storm.hdfs2ftp.util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author xiabaike
 *
 */
public class Hdfs2FtpSQL {
	
	private static final Logger LOG = LoggerFactory.getLogger(Hdfs2FtpSQL.class);

	// 数据库连接管理
	private static DBUtil dbUtil;
	// groupID
	private String groupId;
	
	public Hdfs2FtpSQL(Configure config, String groupId) {
		dbUtil = new DBUtil(config);
		this.groupId = groupId;
	}
	
	/**
	 * 判断groupid是否存在
	 * @return
	 */
	public boolean isExistGroupId() {
		boolean isExist = false;
		String sql = " select group_id from conf_share_rem_info a where group_id = ? ";
		Connection conn = null;
		try {
			conn = dbUtil.getConnection();
			PreparedStatement prep = conn.prepareStatement(sql);
//			prep.setInt(1, Integer.parseInt(groupId));
			prep.setString(1, groupId);
			ResultSet rs = prep.executeQuery();
			if(rs.next()) {
				isExist = true;
			}
		} catch (SQLException e) {
			LOG.error("查询失败，请检查");
			e.printStackTrace();
		} finally{
			// 关闭数据库连接
			dbUtil.close(conn);
		}
		return isExist;
	}
	
	/**
	 * 查询远程及本地路径
	 * @return
	 */
	public Map<String, List<Map<String, String>>> getSharePath() {
		String sql = " select a.loc_id, a.host_key, a.remote_path, b.file_path, a.unit from conf_share_rem_info a ,conf_share_loc_info b "
				+ " where a.loc_id = b.loc_id and a.group_id = '"+groupId+"' and a.is_valid = 0 and b.is_valid = 0 ";
		Connection conn = null;
		Map<String, List<Map<String, String>>> ruleMap = null;
		try {
			ruleMap = new HashMap<String, List<Map<String, String>>>();
			conn = dbUtil.getConnection();
			PreparedStatement prep = conn.prepareStatement(sql);
//			prep.setInt(1, Integer.parseInt(groupId));
//			prep.setString(1, groupId);
			ResultSet rs = prep.executeQuery();
			Map<String, String> map = null;
			List<Map<String, String>> listmap = null;
			String locId = null;
			while(rs.next()) {
				locId = rs.getString("loc_id").trim();
				if(locId.indexOf("_") > -1) {
					locId = locId.substring(locId.lastIndexOf("_") + 1);
					map = new HashMap<String, String>();
					map.put("host_key", rs.getString("host_key").trim());
					map.put("remote_path", formatString(rs.getString("remote_path")));
					map.put("file_path", rs.getString("file_path").trim());
					map.put("unit", rs.getString("unit").trim());
					if(ruleMap.containsKey(locId)) {
						ruleMap.get(locId).add(map);
					}else{
						listmap = new ArrayList<Map<String, String>>();
						listmap.add(map);
						ruleMap.put(locId, listmap);
					}
				}
			}
		} catch (SQLException e) {
			LOG.error("查询失败，请检查");
			e.printStackTrace();
		} finally{
			// 关闭数据库连接
			dbUtil.close(conn);
		}
		return ruleMap;
	}

	private static String formatString(String str) {
		String trim = str.trim();
		return trim.endsWith("/") ? trim : trim+"/";
	}
	
	/**
	 * 查询FTP主机信息
	 * @return
	 */
	public Map<String, Map<String, String>> getFTPHostInfoList(){
		Map<String, Map<String, String>> ftpInfoMap = null;
		String sql = " select distinct c.host_key, c.ip, c.host_name, c.ftp_port, c.ftp_name, c.ftp_pwd "
				+ " from conf_share_rem_info a, conf_host_info c "
				+ " where a.host_key = c.host_key and a.group_id = ? and c.is_valid = 0 ";
		Connection conn = null;
		try {
			ftpInfoMap = new HashMap<String, Map<String, String>>();
			conn = dbUtil.getConnection();
			PreparedStatement prep = conn.prepareStatement(sql);
//			prep.setInt(1, Integer.parseInt(groupId));
			prep.setString(1, groupId);
			ResultSet rs = prep.executeQuery();
//			FTPHostInfo info = null;
			Map<String, String> map = null;
			while(rs.next()) {
				map = new HashMap<String, String>();
				map.put("host_key", rs.getString("host_key").trim());
				map.put("ip", rs.getString("ip").trim());
				map.put("host_name", rs.getString("host_name").trim());
				map.put("ftp_port", rs.getString("ftp_port").trim());
				map.put("ftp_name", rs.getString("ftp_name").trim());
				map.put("ftp_pwd", rs.getString("ftp_pwd").trim());
//				info = new FTPHostInfo();
//				info.setHostKey(rs.getString("host_key"));
//				info.setHostName(rs.getString("host_name"));
//				info.setIp(rs.getString("ip"));
//				info.setFtpPort(rs.getString("ftp_port"));
//				info.setFtpName(rs.getString("ftp_name"));
//				info.setFtpPwd(rs.getString("ftp_pwd"));
				ftpInfoMap.put(rs.getString("host_key"), map);
			}
		} catch (SQLException e) {
			LOG.error("查询失败，请检查");
			e.printStackTrace();
		} finally{
			// 关闭数据库连接
			dbUtil.close(conn);
		}
		return ftpInfoMap;
	}
	
	public static void main(String[] args) {
		Configure config = Configure.getInstance("hdfs2ftp-config.xml");
		Hdfs2FtpSQL sql = new Hdfs2FtpSQL(config, "1");
		System.out.println(sql.getFTPHostInfoList().toString());
	}
	
}
