package com.datacenter.hbase.db;

import java.io.File;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.datacenter.hbase.Util.Checker;
import com.datacenter.hbase.Util.PropertiesLoader;
import com.datacenter.hbase.adapter.HbaseClient;
import com.datacenter.hbase.db.access.MsAccess;
import com.datacenter.hbase.db.excel.MsExcel;

public class DbAccessorFactory {
	private Logger logger = Logger.getLogger(getClass()) ;
	private String TABLES_KEY = "ms.mdb.tables";
	private Set<String> musts = new HashSet<String>() ;
	
	public DbAccessorFactory() throws Exception {
		//get the tables which will be access
		String tables = PropertiesLoader.get(TABLES_KEY, null) ;
		if( null == tables ){
			throw new Exception(TABLES_KEY+" not specified in the hbase_conf.properties") ;
		}
		for(String str : tables.split(";")){
			musts.add( str ) ;
		}
	}
	
	public Set<String> getNeedTables(){
		return this.musts ;
	}
	
	public DbAccessor getAccessor(File file,HbaseClient hClient){
		if( file == null ) {
			return null ;
		}
		String filename = file.getName() ;
		if( Checker.isExcel(filename)){
			return new MsExcel(file,hClient) ;
		}
		if( Checker.isAccess(filename)){
			return new MsAccess(file,getNeedTables(), hClient ) ;
		}
		logger.warn("no db accessor matched with "+filename );
		return new EmptyAccessor() ;
	}
}
