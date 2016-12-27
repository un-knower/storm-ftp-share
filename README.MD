## FTP共享程序 ##
#### 功能 ####
        读取Kafka消息，获取文件路径，根据指定规则，上传至FTP的目录
#### 程序使用说明 ####
        程序主类为：com.bonc.storm.hdfs2ftp.Hdfs2FtpTopology
        程序配置文件：src\main\resources\hdfs2ftp-config.xml
                说明：include-properties 该属性是用来引用其他properties类型的文件，该属性值是文件路径，如：<include-properties>jdbc.properties</include-properties>
                        
        程序参数列表为：
                -topologyName：拓扑名
                -groupID：groupID
                -configPath：配置文件路径
                -ftpType：ftp类型，0为ftp，1为sftp