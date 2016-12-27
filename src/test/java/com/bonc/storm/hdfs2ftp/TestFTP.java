package com.bonc.storm.hdfs2ftp;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketException;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPReply;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class TestFTP {
	
	public static void main(String[] args) {
		FTPClient ftpClient = new FTPClient();
		try {
			ftpClient.connect("192.168.8.52", 2222);
			int reply = ftpClient.getReplyCode();
			if (!FTPReply.isPositiveCompletion(reply)) {
				ftpClient.disconnect();
			}
			boolean result = ftpClient.login("vascilpf", "vasc-ilpf");
			ftpClient.setFileType(FTPClient.BINARY_FILE_TYPE);
			ftpClient.setFileTransferMode(FTP.COMPRESSED_TRANSFER_MODE);
			ftpClient.enterLocalPassiveMode();
			ftpClient.setControlEncoding("UTF-8");
			// 改变工作目录
			boolean ro = ftpClient.changeWorkingDirectory("/itf/qixin/dpi/fix/844/");
			InputStream in = TestFTP.class.getResourceAsStream("/hdfs2ftp.properties");;
			result = ftpClient.storeUniqueFile("hdfs2ftp.properties", in);
			System.out.println(result);
		} catch (SocketException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
//	private void openFile(InputStream inputStream, Path filePath) {
//		int reset = 0;
//		while(reset < 3) {
//			try{
//				// 读文件
//				inputStream = dfs.open(filePath);
//				if(inputStream != null) {
//					reset = 3;
//				}
//			}catch(IOException e) {
//				LOG.info("hdfs connection failed......", e);
//				try {
//					LOG.info("start reconnect hdfs......");
//					hdfsConf = new Configuration();
//					hdfsConf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
//					hdfsConf.set("fs.defaultFS", configMap.get("hdfsUrl"));
//					dfs = FileSystem.get(hdfsConf);
//				} catch (IOException e1) {
//					e1.printStackTrace();
//				}
//				reset++;
//			}
//		}
//	}

}
