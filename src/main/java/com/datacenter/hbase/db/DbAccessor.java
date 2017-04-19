package com.datacenter.hbase.db;

import java.io.File;
import java.sql.Connection;

import com.datacenter.hbase.adapter.HbaseClient;

public interface DbAccessor {
	public static final String CONN_KEY="access.conn";
	public Connection openConn() throws Exception ;
	
	public void access(AccessorOpFactory fac) throws Exception ;
	
	public File getDbFile();
	
	public String getFilePath() ;
	
	public String getDbType();
	
	public void closeConn();
	
	public void setHbaseClient(HbaseClient client);
	
	public HbaseClient getHbaseClient();
}
