package com.datacenter.hbase.phoenix;

import java.sql.Connection;

import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.log4j.Logger;

import com.datacenter.hbase.Util.PropertiesLoader;

public class PhoenixClient { 
	private GenericObjectPool<Connection> connPool ;
	private Logger logger = Logger.getLogger( PhoenixClient.class) ;
	@autoWired("可通过spring注入方式")
	private PhoenixConnectionPoolFactory factory ; //must
	
	private PhoenixClient(PhoenixConnectionPoolFactory factory ) {
		setFactory(factory);
		if( logger.isDebugEnabled() ){
			StringBuilder builder = new StringBuilder("hbase zk configuration:") ;
			builder.append("zk_quorum")
				   .append("=").append( factory.getZkQuorum() ) ;
			logger.debug( builder.toString() );
		}
		GenericObjectPoolConfig config = new GenericObjectPoolConfig() ;
		//config.setTestOnBorrow(true);
		config.setEvictionPolicyClassName("org.apache.commons.pool2.impl.DefaultEvictionPolicy" );
		config.setMaxTotal(PropertiesLoader.getInt("habse.maxconn", 100));
		config.setMinIdle( PropertiesLoader.getInt("habse.min", 10) ); //habse.min
		connPool = new GenericObjectPool<Connection>( getFactory() ) ;
	}
	
	public static PhoenixClient build(PhoenixConnectionPoolFactory factory){
		PhoenixClient cli = new  PhoenixClient(factory) ;
		return cli;
	}
	
	public Connection openConnection() throws Exception{
		 return connPool.borrowObject() ;
	}
	/**
	 * PHOENIX connection do not need be reused
	 * @param conn
	 * @throws Exception
	 */
	public void releaseConnection(Connection conn) throws Exception{
		 connPool.invalidateObject( conn);
	}

	public PhoenixConnectionPoolFactory getFactory() {
		return this.factory;
	}

	public void setFactory(PhoenixConnectionPoolFactory factory) {
		this.factory = factory;
	}
}
