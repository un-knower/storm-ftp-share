package com.bonc.storm.hdfs2ftp;

import com.bonc.storm.hdfs2ftp.bean.FTPMessage;
import com.bonc.storm.hdfs2ftp.bean.MessageType;
import com.bonc.storm.hdfs2ftp.ftp.*;
import com.bonc.storm.hdfs2ftp.util.DateUtil;
import com.bonc.storm.hdfs2ftp.util.SystemHelper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.jcraft.jsch.ChannelSftp;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.pool.KeyedObjectPool;
import org.apache.commons.pool.KeyedPoolableObjectFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.TridentOperationContext;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;



/**
 * 本程序是为了实现从kafka消息中获取文件及路径信息，根据规则发送到相应FTP服务器指定目录下
 * @author xiabaike
 */
public class Hdfs2FtpFunction  extends BaseFunction{

	private static final long serialVersionUID = -1592510638631523911L;
	private static final Logger LOG = LoggerFactory.getLogger(Hdfs2FtpFunction.class);
	// 加载配置信息
	private Map<String, String> configMap = null;
	// hadoop配置信息
	private Configuration hdfsConf = null;
	// hdfs文件系统
	private FileSystem dfs = null;
	// FTP客户端工厂
	private KeyedPoolableObjectFactory facotry = null;
	// FTP客户端连接池
	private KeyedObjectPool pool;
	// 本地路径，远程FTP及远程目录
	private Map<String, List<Map<String, String>>> shareMap = null;
	// FTP主机信息
	private Map<String, Map<String, String>> ftpInfoMap = null;

	private FTPMessage ftpMsg = null;
	// 当前主机的IP地址
	private String locIP = null;
	// FTP服务器类型，0为FTP，1为SFTP，默认是FTP
	private String ftpType = "0";
	private Pattern pattern = null;
	private Matcher matcher = null;

	private boolean krblag;
	private String krbPrincipal;
	private String krbKeystore;
	private Gson gsonPasser;
	private Type mapType;
	public Hdfs2FtpFunction(String ftpType) {
		this.ftpType = ftpType;
	}
	
	@SuppressWarnings("unchecked")
	@Override
    public void prepare(Map conf, TridentOperationContext context) {
		this.configMap = (Map<String, String>) conf.get("hdfs2ftp.config");
		this.shareMap = (Map<String, List<Map<String, String>>>) conf.get("share.path");
		this.ftpInfoMap = (Map<String, Map<String, String>>) conf.get("ftp.host.info");

		this.krblag = Boolean.parseBoolean( (String)conf.get("krb_flag") ) ;
		if (krblag) {
			this.krbPrincipal = (String) conf.get("krb_principal");
			this.krbKeystore = (String) conf.get("krb_keystore");
		}
		try {
			if("0".equals(ftpType)) {
				facotry = new FTPClientFactory(ftpInfoMap, configMap);
				pool = new FTPClientPool(ftpInfoMap, facotry);
			}else{
				facotry = new SFTPClientFactory(ftpInfoMap, configMap);
				pool = new SFTPClientPool(ftpInfoMap, facotry);
			}
			if(configMap.get("hdfsUrl") != null){
				LOG.info("hafs配置如下，fs.default.url：{}",configMap.get("hdfsUrl"));
				hdfsConf = new Configuration();
				hdfsConf.addResource("hdfs-site.xml");
				hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
				if( krblag ){
					LOG.info(">>>>>>>>>Enable Kerberos>>>>>>>");
					hdfsConf.set("hadoop.security.authentication", "Kerberos");
					UserGroupInformation.setConfiguration(hdfsConf);
					LOG.info("the KRB_PRINCPAL is {} and the KRB_KEYSTORE is {}", krbPrincipal, krbKeystore);
					UserGroupInformation.loginUserFromKeytab(krbPrincipal, krbKeystore);
				}
//				hdfsConf.set("hadoop.job.ugi", "");
//				hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//				hdfsConf.set("fs.defaultFS", configMap.get("hdfsUrl"));
				dfs = FileSystem.get(hdfsConf);
			}else{
				LOG.info("请检查 配置文件中 hdfs 配置 hdfsUrl");
			}
			// 获取当前系统的IP地址
			locIP = SystemHelper.getLocalIPForJava();
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e1) {
			e1.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		String interval = configMap.get("ftp.monitor.sleep.interval");
		if(!"".equals(interval) && interval != null) {
			// ftp连接监视器
			FTPClientMonitor monitor = new FTPClientMonitor(pool, facotry, ftpInfoMap, Long.parseLong(interval));
			Thread thread = new Thread(monitor, "FTPCLient monitor");
			// 设置为后台线程
			thread.setDaemon(true);
			// 启动后台线程
			thread.start();
		}
		this.gsonPasser = new Gson();
		this.mapType = new MapType().getType();
	}
	
	public void execute(TridentTuple tuple, TridentCollector collector) {
		//long start = System.currentTimeMillis();
		String message = (String) tuple.get(0);
		if(message == null || "".equals(message)) {
			return;
		}
		// 上传开始时间
		String beginTime = null;
		String filename = null;
		Map<String,String> jsonMap = null;
		ftpMsg = new FTPMessage();
		InputStream in = null;
		String hostKey = null;
		String remotePath = null;
		try {
			jsonMap = this.gsonPasser.fromJson(message, this.mapType);
		} catch (Exception e) {
			e.printStackTrace();
			return;
		}

		try {
			jsonMap = this.gsonPasser.fromJson(message, this.mapType);
			String provId = jsonMap.get("prov_id");
			if("".equals(provId)||provId==null) {
				return ;
			}
			filename = jsonMap.get("file_name");
			ftpMsg.setFileName(filename);
			String filePathStr = jsonMap.get("file_path");
			// 根据规则中的文件路径正则匹配该文件路径是否符合规则，找出相对应的host_key和远程路径
			List<Map<String, String>> mapList = shareMap.get(provId);
			if(mapList == null)
				return ; 
			
			String unit = null;
			Map<String, String> map = null;
			for(int i = 0; i < mapList.size(); i++) {
				map = mapList.get(i);
				pattern = Pattern.compile(map.get("file_path"));
				matcher = pattern.matcher(filePathStr);
				if(matcher.find()) {
					hostKey = map.get("host_key");
					remotePath = map.get("remote_path");
					unit = map.get("unit");
					break;
				}
			}
			
			// 如果匹配到了，把该文件上传至host_key对应的ftp上的远程路径中
			if(!"".equals(hostKey) && hostKey != null && !"".equals(remotePath) && remotePath != null) {
				LOG.info("match topic: {}", message);
				
				ftpMsg.setOpTime(jsonMap.get("op_time"));
				ftpMsg.setProvId(jsonMap.get("prov_id"));
				ftpMsg.setLocIP(locIP);
				ftpMsg.setUnit(unit);
				ftpMsg.setLocPath(filePathStr);
				ftpMsg.setHjBeginTime(jsonMap.get("begin_time"));
				ftpMsg.setHjEndTime(jsonMap.get("end_time"));
				ftpMsg.setTid(jsonMap.get("tid"));
				ftpMsg.setFtpName(ftpInfoMap.get(hostKey).get("ftp_name"));
				ftpMsg.setRemoteIP(ftpInfoMap.get(hostKey).get("ip"));
				ftpMsg.setReloadFlag(jsonMap.get("reload_flag") == null ? "" : jsonMap.get("reload_flag"));
				
				// 上传开始时间
				beginTime = DateUtil.getNowTime("yyyyMMddHHmmssS");
				if(remotePath.indexOf("${YYYYMMDD}") > 0) {
					remotePath = remotePath.replace("${YYYYMMDD}", beginTime.substring(0, 8));
				}else if(remotePath.indexOf("${YYYYMM}") > 0) {
					remotePath = remotePath.replace("${YYYYMM}", beginTime.substring(0, 6));
				}else if(remotePath.indexOf("${YYYY}") > 0) {
					remotePath = remotePath.replace("${YYYY}", beginTime.substring(0, 4));
				}
				ftpMsg.setRemotePath(remotePath);
			
				Path filePath = new Path(filePathStr);
				// 读文件
				in = openFileFromHdfs(filePath);
//				in = dfs.open(filePath);
				long fileSize = dfs.getFileStatus(filePath).getLen();
				ftpMsg.setFileSize(String.valueOf(fileSize));
				boolean result = false;
				String remark = "";
				if("0".equals(ftpType)) {
					int retryCounter = 0;
					while(retryCounter < 3) {
						// 从ftp连接池中取出相对应的连接对象
						LOG.info("{} begin get ftpclient....{}",retryCounter, DateUtil.getNowTime("yyyyMMddHHmmssS"));
						FTPClient ftpClient = (FTPClient) pool.borrowObject(hostKey);
						LOG.info("{} end get ftpclient....{}", retryCounter, DateUtil.getNowTime("yyyyMMddHHmmssS"));
						// 改变工作目录
						boolean ro = ftpClient.changeWorkingDirectory(remotePath);
						LOG.info("{} end changeHomeDirectory ....{}", retryCounter, DateUtil.getNowTime("yyyyMMddHHmmssS"));
						if(!ro) {
							LOG.info("begin makeDirectory  ...."+ DateUtil.getNowTime("yyyyMMddHHmmssS"));
							boolean make = ftpClient.makeDirectory(remotePath);
							if(!make) {
								LOG.error("create directory {} failed, permission denied......", remotePath);
								remark = remotePath+" 目录权限不足";
								break;
							}
							ro = ftpClient.changeWorkingDirectory(remotePath);
							LOG.info("end makeDirectory  ...."+ DateUtil.getNowTime("yyyyMMddHHmmssS"));
						}
						// 写入FTP，并指定文件名
						LOG.info("{} begin storeFile  ....{}", retryCounter, DateUtil.getNowTime("yyyyMMddHHmmssS"));
						result = ftpClient.storeFile(filename, in);
						LOG.info("result: {}", result);
						LOG.info("{} end storeUniqueFile  ....{}", retryCounter, DateUtil.getNowTime("yyyyMMddHHmmssS"));
						if(result) {
							retryCounter = 3;
						}else{
							retryCounter++;
						}
					}
				}else{
					// 从sftp连接池中取出相对应的连接对象
					ChannelSftp sftp = (ChannelSftp) pool.borrowObject(hostKey);
					// 判断目录是否存在
					if( !isExistDir(remotePath, sftp) ) {
						sftp.mkdir(remotePath);
						// 改变工作目录
						sftp.cd(remotePath);
					}
					// 上传文件，并指定传输模式
					sftp.put(in, filename, ChannelSftp.RESUME);
					result = true;
				}
				// 上传结束时间
				String endTime = DateUtil.getNowTime("yyyyMMddHHmmssS");
				ftpMsg.setBeginTime(beginTime);
				ftpMsg.setEndTime(endTime);
				if(result) {
					// 设置操作时间
					ftpMsg.setOperTime(DateUtil.getNowTime("yyyyMMddHHmmssS"));
					LOG.info("begin send normal message  ...."+ DateUtil.getNowTime("yyyyMMddHHmmssS"));
					collector.emit(new Values(MessageType.NORMAL, filename, ftpMsg.toJSONComm()));
					LOG.info("end send normal message  ...."+ DateUtil.getNowTime("yyyyMMddHHmmssS"));
				}else{
					// 失败原因
					if("".equals(remark)) {
						remark = "写入FTP主机失败";
					}
					/*// 删除共享异常文件
					if(deleteErrorFile(hostKey, remotePath, filename)) {
						LOG.info("delete file of share - {} is successful....", filename);
					}else{
						LOG.info("delete file of share - {} is failed....", filename);
					}*/
					ftpMsg.setRemark(remark);
					LOG.info("File {} write to FTPServer {} directory {} failed......", filePathStr, hostKey, remotePath);
					// 设置操作时间
					ftpMsg.setOperTime(DateUtil.getNowTime("yyyyMMddHHmmssS"));
					collector.emit(new Values(MessageType.ERROR, filename, ftpMsg.toJSONError()));
				}
			}
		} catch (Exception e) {
//			ftpMsg.setBeginTime(beginTime);
			// 是否补传标识
			ftpMsg.setReloadFlag(jsonMap.get("reload_flag") == null ? "" : jsonMap.get("reload_flag"));
			// 失败原因
			ftpMsg.setRemark(e.toString());
			// 设置操作时间
			ftpMsg.setOperTime(DateUtil.getNowTime("yyyyMMddHHmmssS"));
			// 删除共享异常文件
			/*if(deleteErrorFile(hostKey, remotePath, filename)) {
				LOG.info("delete file of share - {} is successful....", filename);
			}else{
				LOG.info("delete file of share - {} is failed....", filename);
			}*/
			collector.emit(new Values(MessageType.ERROR, filename, ftpMsg.toJSONError()));
			LOG.error("error message:{}", message, e);
		}finally {
			if(in != null) {
				try {
					in.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	@Override
    public void cleanup() {
		try {
			LOG.info("cleanup  关闭池，释放所有与它相关资源 ");
			// 关闭池，释放所有与它相关资源 
			if(pool != null) {
				pool.close();
			}
			if(dfs != null) {
				dfs.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
    }
	
	/**
	 * 共享失败，删除对端主机上的异常文件
	 */
	private boolean deleteErrorFile(String hostKey, String remotePath, String filename) {
		boolean result = false;
		try {
			if("0".equals(ftpType)) {
				int retryCounter = 0;
				while(retryCounter < 3) {
					FTPClient ftpClient = (FTPClient) pool.borrowObject(hostKey);
					LOG.info("start delete file of share - {} .......", filename);
					// 改变工作目录
					boolean ro = ftpClient.changeWorkingDirectory(remotePath);
					if(ro) {
						result = ftpClient.deleteFile(filename);
						if(result) {
							retryCounter = 3;
						}else{
							retryCounter++;
						}
					}
				}
			}else{
				// 从sftp连接池中取出相对应的连接对象
				ChannelSftp sftp = (ChannelSftp) pool.borrowObject(hostKey);
				// 改变工作目录
				sftp.cd(remotePath);
				sftp.rm(filename);
				result = true;
			}
		} catch (NoSuchElementException e) {
			e.printStackTrace();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	/**
	 * 从hdfs中读取文件，如果没有读取到或者抛出异常，重试连接3次
	 */
	private InputStream openFileFromHdfs(Path filePath) {
	int reset = 0;
	InputStream inputStream = null;
	while(reset < 3) {
		try{
			// 读文件
			inputStream = dfs.open(filePath);
			if(inputStream != null) {
				reset = 3;
			}else{
				// 读取文件为空，尝试重新连接HDFS
				LOG.info("start reconnect hdfs......");
				reset++;
				hdfsConf = new Configuration();
				hdfsConf.addResource("hdfs-site.xml");
				hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//				hdfsConf.set("fs.defaultFS", configMap.get("hdfsUrl"));
				dfs = FileSystem.get(hdfsConf);
			}
		}catch(FileNotFoundException e) {
			reset = 3;
			LOG.error("File does not exist : {}", filePath.toUri().toString(), e);
		}catch(IOException e) {
			LOG.info("hdfs connection failed......", e);
			try {
				LOG.info("start reconnect hdfs......");
				reset++;
				hdfsConf = new Configuration();
				hdfsConf.addResource("hdfs-site.xml");
				hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//				hdfsConf.set("fs.defaultFS", configMap.get("hdfsUrl"));
				dfs = FileSystem.get(hdfsConf);
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
	}
	return inputStream;
}
	
	/**
	 * 判断目录是否存在
	 */
	public static boolean isExistDir(String directory, ChannelSftp sftp) {
		try {
			// 改变工作目录
			sftp.cd(directory);
			return true;
		} catch (Exception e) {
			LOG.error("没有目录 ： {}", directory);
			return false;
		}
	}

	public static class MapType extends TypeToken<Map<String, String>> {
	}
}
