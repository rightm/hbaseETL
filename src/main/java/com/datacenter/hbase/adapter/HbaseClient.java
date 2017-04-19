package com.datacenter.hbase.adapter;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.log4j.Logger;

import com.datacenter.hbase.Util.PropertiesLoader;

/**
 * connection the Hbase server and add data
 * @author Administrator
 *
 */
public class HbaseClient {
	private Configuration conf ;
	private GenericObjectPool<Connection> connPool ;
	private Logger logger = Logger.getLogger( HbaseClient.class) ;
	
	public static void main(String[] args) {
		 
	}
	
	private HbaseClient(Configuration conf) {
		this.conf = conf ;
		if( logger.isDebugEnabled() ){
			StringBuilder builder = new StringBuilder("hbase configuration:") ;
			builder.append("zk_pool").append("=").append(conf.get(HbaseAdapter.HBASE_PORT))
				   .append(";").append("zk_quorum")
				   .append("=").append( conf.get(HbaseAdapter.HABSE_QUORUM)) ;
			logger.debug( builder.toString() );
		}
		GenericObjectPoolConfig config = new GenericObjectPoolConfig() ;
		//config.setTestOnBorrow(true);
		config.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy" );
		config.setMaxTotal(PropertiesLoader.getInt("habse.maxconn", 5));
		config.setMinIdle( PropertiesLoader.getInt("habse.min", 3) ); //habse.min
		connPool = new GenericObjectPool<Connection>(new HbaseConnectionPoolFactory(this)) ;
	}
	
	public static HbaseClient build(){
		Configuration conf = HBaseConfiguration.create();
		String hbasePort = PropertiesLoader.get(HbaseAdapter.HBASE_PORT, "2181") ;
		String hbaseZk = PropertiesLoader.get(HbaseAdapter.HABSE_QUORUM, "localhost") ;
//		conf.set("hbase.zookeeper.property.clientPort", "2181");
//		conf.set("hbase.zookeeper.quorum", "192.168.0.130");
		
		conf.set(HbaseAdapter.HBASE_PORT, hbasePort);
		conf.set(HbaseAdapter.HABSE_QUORUM, hbaseZk);
		return new  HbaseClient(conf);
	}
	
	public Connection openConnection() throws Exception{
		 return connPool.borrowObject() ;
	}
	
	public void releaseConnection(Connection conn) throws Exception{
		 connPool.returnObject(conn);
	}

	public Configuration getConf() {
		return conf;
	}

	public void setConf(Configuration conf) {
		this.conf = conf;
	}
}
