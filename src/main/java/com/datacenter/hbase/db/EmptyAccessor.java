package com.datacenter.hbase.db;

import java.io.File;
import java.sql.Connection;

import org.apache.log4j.Logger;

import com.datacenter.hbase.adapter.HbaseClient;

public class EmptyAccessor implements DbAccessor {
	private Logger logger = Logger.getLogger(getClass()) ;
	public Connection openConn() throws Exception {
		// TODO Auto-generated method stub
		return null ;
	}

	public void access(AccessorOpFactory fac) throws Exception {
		logger.info("empty accessor...");
	}

	public File getDbFile() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getDbType() {
		// TODO Auto-generated method stub
		return null;
	}

	public void closeConn() {
		// TODO Auto-generated method stub
		
	}

	public void setHbaseClient(HbaseClient client) {
		// TODO Auto-generated method stub
		
	}

	public HbaseClient getHbaseClient() {
		 return null ;
	}

	public String getFilePath() {
		// TODO Auto-generated method stub
		return null;
	}

}
