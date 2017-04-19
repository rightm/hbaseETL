package com.datacenter.hbase.adapter;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HbaseConnectionPoolFactory implements PooledObjectFactory<Connection>{
	private HbaseClient client ;
	
	public HbaseConnectionPoolFactory(HbaseClient client) {
		this.client = client ;
	}
	
	public PooledObject<Connection> makeObject() throws Exception {
		if ( null != client && null !=client.getConf() ) {
			Connection conn = ConnectionFactory.createConnection(client.getConf()) ;
			return new DefaultPooledObject<Connection>(conn) ;
		}
		throw new NullPointerException("no congifuration in the hbaseclient") ;
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
			return !conn.isAborted() && !conn.isClosed() ;
		}
		return false;
	}

	public void activateObject(PooledObject<Connection> p) throws Exception {
		if( null == p) return ;
		Connection conn = p.getObject() ;
		if( conn.isClosed() || conn.isAborted() ){
			//make a new one
			conn = makeObject().getObject() ;
		}
	}

	public void passivateObject(PooledObject<Connection> p) throws Exception {
		 //do nothing
	}

	public HbaseClient getClient() {
		return client;
	}

	public void setClient(HbaseClient client) {
		this.client = client;
	}

}
