package com.datacenter.hbase.phoenix;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.apache.log4j.Logger;

import com.datacenter.hbase.Util.Checker;

public class PhoenixBaseDao {
	private PhoenixClient client ;
	private Logger logger = Logger.getLogger( getClass() ) ;
	
	public PhoenixBaseDao() {
		 
	}
	
	public PhoenixBaseDao(PhoenixClient client) {
		this.client = client ;
	}
	
	public Connection getConn() throws  Exception{
		return client.openConnection() ;
	}
	/**
	 * 建表，请手动创建，避免出现问题
	 * @deprecated
	 */
	public boolean createTable(String tableName,List<String> columns){
		return false ;
	}
	
	public int insert(String sql){
	    Connection conn = null;
	    Statement statement = null ;
	    ResultSet rs = null ;
	    int count = 0 ;
	 
        try {
        	conn = getConn();
            statement = conn.createStatement();
           // String sql = "select count(1) as num from STOCK_SYMBOL";
           // String sql = "UPSERT INTO STOCK_SYMBOL(SYMBOL,COMPANY) VALUES('a','b')";
            count= statement.executeUpdate(sql) ;
            conn.commit();
            return count ;
        } catch ( Exception e) {
        	logger.error("can't insert",e);
        }finally{
        	Checker.gracefullyClose(rs);
        	Checker.gracefullyClose(statement);
        	try {
				client.releaseConnection(conn);
			} catch (Exception e) {
				logger.error("can't release the conn",e);
			}
        }
		return count;
	}

	public PhoenixClient getClient() {
		return client;
	}

	public void setClient(PhoenixClient client) {
		this.client = client;
	}
	
	
}
