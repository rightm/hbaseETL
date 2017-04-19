package com.datacenter.hbase.db.access;

import java.io.File;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSONObject;
import com.datacenter.hbase.Util.Checker;
import com.datacenter.hbase.Util.HbaseUtil;
import com.datacenter.hbase.Util.PropertiesLoader;
import com.datacenter.hbase.adapter.HbaseCell;
import com.datacenter.hbase.adapter.HbaseClient;
import com.datacenter.hbase.db.AccessorOpFactory;
import com.datacenter.hbase.db.AccessorOperation;
import com.datacenter.hbase.db.DBConstants;
import com.datacenter.hbase.db.DbAccessor;
import com.datacenter.hbase.db.hbasehandler.IHbandler;
import com.datacenter.hbase.db.hbasehandler.MsAccessHandler;
import com.datacenter.hbase.db.htable.intf.HtableFactory;
import com.datacenter.hbase.db.htable.intf.ITable;

public class MsAccess implements DbAccessor {
	private Logger logger = Logger.getLogger(getClass()) ;
	private volatile boolean isOpened = false;
	private Connection conn ;
	private File file ;
	private List<String> tableNames = new ArrayList<String>() ; //表名称
	private Set<String> needed  ;  //需要的表 
	private HbaseClient hClient ;
	private String filepath ;
	
	public MsAccess(File file ,Set<String> needed,HbaseClient hClient) {
		this.file = file ;
		if( null != file){
			this.filepath = file.getAbsolutePath() ;
		}
		this.needed = needed ;
		this.hClient = hClient ;
	}
	
	private Connection getConn(){
		return openConn() ;
	}
	
	public void closeConn() {
		 Checker.gracefullyClose(this.conn);
	}
	/**
	 * @private
	 */
	public Connection openConn() {
		 
		Properties prop = new Properties();
		prop.put("charSet", "gb2312"); // 这里是解决中文乱码
		prop.put("user", "");
		prop.put("password", "");
		String filePath ;
		if( null != this.file ){
			filePath = this.file.getAbsolutePath() ;
		}else{
			filePath = PropertiesLoader.get("file.path", null) ;//
		}
		//logger.info("Ms Access file.path:"+filePath);
		String url = "jdbc:odbc:driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + filePath; // 文件地址
		try {
			logger.debug(">>>>>>>>>>>>connection url "+ url);
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
			conn = DriverManager.getConnection(url, prop);
			isOpened = true ;
			return conn ;
		} catch (Exception e) {
			logger.error("can't load ms access driver");
			throw new IllegalArgumentException("check the *.mdb exists in "+filePath) ;
		}
	}
	
	/**
	 * 获取该表的总记录数目
	 * @return
	 * @private
	 */
	public long getRows(String table){
		Connection conn = null ;
		PreparedStatement ps = null;
		Statement stmt = null;
		ResultSet rs = null;
		try {
			conn = getConn() ;
			stmt = (Statement) conn.createStatement();
			rs = stmt.executeQuery("select count(*) from "+table);//first line
			
			while (rs.next()) {
				return rs.getLong(1) ;
			}
		} catch (Exception e) {
			logger.error("can't get data",e);
		}finally{
			Checker.gracefullyClose(rs);
			Checker.gracefullyClose(stmt);
			Checker.gracefullyClose(conn);
		}
	     return 0 ;
	}
	/**
	 * 获取所有的表
	 * @private
	 * @throws Exception
	 */
	public void getAllTables() throws Exception{
		 Connection conn= getConn() ;   
	     Statement stmt= null;  
	     ResultSet  rs =null ;
	     if( null == conn ){
	    	 throw new NullPointerException("no connection initialized yet") ;
	     }
	     try {
	    	 stmt=conn.createStatement();   
			 DatabaseMetaData  dbmd=conn.getMetaData();    
			 rs=dbmd.getTables(null,null,"%",null);    
			 while(rs.next()){   
				 String tableName = rs.getString(3) ;
				 //System.out.println("table-name:  "+tableName+"");  
				 if( needed.contains( tableName )){
					 tableNames.add( tableName ) ;
				 }
			 }
		} catch ( Exception e) {
			logger.error("can not get table names",e );
		}finally{
			Checker.gracefullyClose( rs ); 
			Checker.gracefullyClose( stmt );   
			Checker.gracefullyClose(conn);
		} 
	 
	}
	/**
	 * get columns in sequence
	 * @private
	 */
	public List<String> getColumnNames(String table){
			Connection conn = null ;
			PreparedStatement ps = null;
			Statement stmt = null;
			ResultSet rs = null;
			List<String> columns = new ArrayList<String>() ;
			try {
				conn = getConn() ;
				stmt = (Statement) conn.createStatement();
				rs = stmt.executeQuery("select TOP 1 * from "+table);//first line
				ResultSetMetaData data = rs.getMetaData();
//				DatabaseMetaData dbmd = conn.getMetaData();
//	            rs = dbmd.getSchemas();   
				
				while (rs.next()) {
					for (int i = 1; i <= data.getColumnCount(); i++) {
						String columnName = data.getColumnName(i); // 列名
						columns.add( columnName ) ;
					}
				}
			} catch (Exception e) {
				logger.error("can't get data",e);
			}finally{
				Checker.gracefullyClose(rs);
				Checker.gracefullyClose(stmt);
				Checker.gracefullyClose(conn);
			}
			return columns ;
	}
	
	public void access(AccessorOpFactory fac) throws Exception {
//		openConn() ;
//		if( null == conn){
//			logger.warn( "no active ms access connection");
//			return ;
//		}
		Map<String, Object> param = new HashMap<String, Object>() ;
		//param.put(DbAccessor.CONN_KEY, getConn() ) ;
		getAllTables();
		for(String table:tableNames){
			logger.debug("<access>get table:"+table);
			AccessorOperation op = fac.getOperation(this,table) ;
			if( null == op ) {
				//skip the no operation specified tables
				continue ;
			}
			IHbandler handler = new MsAccessHandler(this.hClient) ;
			param.put( tableNameKey() ,  table) ;
			param.put( "hclient" ,  handler) ;
			//使用具体的连接，对某张表做处理
			op.acccess(this,param ) ;	
		}
		closeConn();
	}

	public static String tableNameKey(){
		return "table_name" ;
	}
	
	public String getDbType() {
		return DBConstants.MS_ACCESS;
	}

	public File getDbFile() {
		return this.file;
	}

	public List<String> getTableNames() {
		return tableNames;
	}

	public void setTableNames(List<String> tableNames) {
		this.tableNames = tableNames;
	}

	public void setHbaseClient(HbaseClient client) {
		this.hClient = client ;
	}

	public HbaseClient getHbaseClient() {
		return this.hClient ;
	}

	public String getFilepath() {
		return filepath;
	}

	public void setFilepath(String filepath) {
		this.filepath = filepath;
	}

	public String getFilePath() {
		return getFilepath()  ;
	}

	public File getFile() {
		return file;
	}

	public void setFile(File file) {
		this.file = file;
	}

}
