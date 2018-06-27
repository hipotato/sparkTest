package com.potato.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class HBaseConManager {
	private static HTable table = null;
	//中转库连接配置
	private Configuration transferConf=null;
	//核心库连接配置
	private Configuration coreConf=null;
	
	private Connection coreConnection=null; 
	
	private Connection transConnection=null;
	
	private static HBaseConManager instance=new HBaseConManager();
	public static HBaseConManager getInstance(){
		return instance;
	}
	
	private HBaseConManager(){
		transferConf=new Configuration();
		transferConf.addResource("hbase-site-transfer.xml");
		transferConf= HBaseConfiguration.create(transferConf);
		transferConf.set("hbase.client.operation.timeout", "3000"); 
		transferConf.set("hbase.client.retries.number", "2");
		transferConf.set("zookeeper.recovery.retry", "1");
		transferConf.set("hbase.rpc.timeout", "1000");
		transferConf.set("zookeeper.session.timeout", "10000");
		transferConf.set("hbase.zookeeper.quorum","pro1,pro2,pro3");
		transferConf.set("hbase.rootdir","hdfs://pro1:9000/regionServers");
		
		coreConf=HBaseConfiguration.create();
		coreConf.addResource("hbase-site-core.xml");
		coreConf= HBaseConfiguration.create(coreConf);
		coreConf.set("hbase.client.operation.timeout", "3000"); 
		coreConf.set("hbase.client.retries.number", "2");
		coreConf.set("zookeeper.recovery.retry", "1");
		coreConf.set("hbase.rpc.timeout", "1000");
		coreConf.set("zookeeper.session.timeout", "10000");
		coreConf.set("hbase.zookeeper.quorum","pro1,pro2,pro3");
		coreConf.set("hbase.rootdir","hdfs://pro1:9000/regionServers");

		try {
			transConnection = ConnectionFactory.createConnection(transferConf);
			coreConnection =  ConnectionFactory.createConnection(coreConf);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	/**
	 * 获取HTable示例
	 * @param clusterId：集群编号，0：核心库，1：中转库
	 * @param name：表名
	 * @return HTable示例
	 * @throws IOException
	 */
	public Table getHTableInstanceByName(int clusterId, String name) throws IOException {
		if(clusterId==0){
			return coreConnection.getTable(TableName.valueOf(name));
		}else {
			return transConnection.getTable(TableName.valueOf(name));
		}
	}

	public Configuration getTransferConf() {
		return transferConf;
	}

	public Configuration getCoreConf() {
		return coreConf;
	}
}
 