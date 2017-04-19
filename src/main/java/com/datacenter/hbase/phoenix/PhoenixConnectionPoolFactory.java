package com.datacenter.hbase.phoenix;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.log4j.Logger;

import com.datacenter.hbase.Util.PropertiesLoader;

public class PhoenixConnectionPoolFactory implements PooledObjectFactory<Connection>{
	private Logger logger = Logger.getLogger(getClass()) ;
	private List<String> zkQuorum; //spring configuration must
	private int index =0 ; 
	
	public PhoenixConnectionPoolFactory( ) {
		if( null == getZkQuorum() ){
			//connStr=54.152.31.122:2181,54.152.31.123:2181,54.152.31.124:2181
			String connStr = PropertiesLoader.get("phoenix.zkquorum", "localhost:2181") ;
			setZkQuorum(  Arrays.asList( connStr.split(",") ) );
			logger.info("phoenix.zkquorum："+getZkQuorum() );
		}
	}
	/**
	 * get a connection of phoenix jdbc
	 * @return
	 */
	public Connection getJdbcConnection()throws Exception {
	    Connection conn = null;
	    String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
	    //jdbc:phoenix:54.152.31.122,54.152.31.123,54.152.31.124;
	    String url = "jdbc:phoenix:"+getZkQuorum().get(index%getZkQuorum().size())+"";
	    if( null == getZkQuorum() ){
	    	throw new IllegalArgumentException("zkQuorum not specified in PhoenixConnectionPoolFactory") ;
	    }
	    try {
    		Class.forName(driver);
        	conn = DriverManager.getConnection(url);
        	return conn ;
        } catch ( Exception e) {
        	logger.error("can not get a phoenix-jdbc connection",e);
        	try {
        		++index ;
        		//try one more times ;
				return getJdbcConnection() ;
			} catch (Exception e1) {
				throw new IllegalArgumentException(e) ;
			}
        } 
	}
	
	public PooledObject<Connection> makeObject() throws Exception {
		if ( null != getZkQuorum() ) {
			 return new DefaultPooledObject<Connection>( getJdbcConnection() ) ;
		}
		throw new NullPointerException("no zkQuorum specified") ;
	}

	public void destroyObject(PooledObject<Connection> p) throws Exception {
		if( null != p){
			Connection conn = p.getObject() ;
			if( null != conn && !conn.isClosed() ){
				conn.close();
			}
		}
		
	}

	public boolean validateObject(PooledObject<Connection> p) {
		if( null != p ){
			Connection conn = p.getObject() ;
			try {
				return !conn.isClosed() ;
			} catch (SQLException e) {
				logger.error("phoenix connection is not closed yet",e);
			}
		}
		return false;
	}

	public void activateObject(PooledObject<Connection> p) throws Exception {
		if( null == p) return ;
		Connection conn = p.getObject() ;
		if( conn.isClosed()){
			//make a new one
			conn = makeObject().getObject() ;
		}
	}

	public void passivateObject(PooledObject<Connection> p) throws Exception {
		 //do nothing
	}
	public List<String> getZkQuorum() {
		return zkQuorum;
	}
	public void setZkQuorum(List<String> zkQuorum) {
		this.zkQuorum = zkQuorum;
	}

}
