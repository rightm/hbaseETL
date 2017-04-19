package com.datacenter.hbase.db.access;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.datacenter.hbase.Util.Checker;
import com.datacenter.hbase.Util.HbaseUtil;
import com.datacenter.hbase.adapter.HbaseCell;
import com.datacenter.hbase.db.AccessorOperation;
import com.datacenter.hbase.db.DbAccessor;
import com.datacenter.hbase.db.hbasehandler.IHbandler;
import com.datacenter.hbase.db.htable.intf.HtableFactory;
import com.datacenter.hbase.db.htable.intf.ITable;

/**
 * 必须使用id来遍历，即  id > xx and id < xx
 * @author Administrator
 * @deprecated
 */
public class DefaultMsAccessOperation implements AccessorOperation{
	private Logger logger = Logger.getLogger(getClass()) ;
	private int start = 0 ;
	private int end = 100 ;
	private String filePath ;
	
	public int getEnd() {
		return end;
	}

	public void setEnd(int end) {
		this.end = end;
	}

	private int offset = 1000 ;
	private long size = 0 ;  //表的总记录数目
	private String where ; //where ID > "+start+" and ID < 100"
	
	public DefaultMsAccessOperation(String where,int start,int offset) {
		 
	}
	
	/**
	 * 根据主键 和分页参数返回对应的sql
	 * @param tableName
	 * @param currPage
	 * @param pk
	 * @return
	 */
	private String getSQL(String tableName,int currPage,String pk){
		int range = (getOffset()*(currPage-1)) ;
		String subSql = "" ;
		String id = "id" ;
		if( null != pk && !"".equals( pk ) ){
			id = pk ;
		}
		
		if( range == 0 ){
			subSql = "";
		}else{
			subSql = " where "+id+" not in (select top "+range+" "+id+" from "+tableName+" order by "+id+" ) " ;
		}
		return "select top "+getOffset()+" * from "+tableName+subSql+" order by "+id+" " ;
	}
	
	/**
	 * 整表处理
	 * @throws Exception 
	 */
	public <T> List<T> acccess(DbAccessor acc,Map<String, Object> parameter) throws Exception {
		//Connection conn = (Connection) parameter.get(DbAccessor.CONN_KEY) ;
		String tableName = (String) parameter.get( MsAccess.tableNameKey()) ;
		IHbandler handler = (IHbandler) parameter.get( "hclient" ) ;
		ITable tableInfor = HtableFactory.getItable(tableName) ;
		MsAccess msAcc = (MsAccess) acc ;
		
		handler.setHtable(tableInfor);
		 
		setSize(msAcc.getRows(tableName) );
		logger.debug(tableName+".size="+getSize());
		if( where == null ){
			setWhere( tableInfor.getWhere( ""+start,""+offset ) );
		}
		
		try{
		// 1.创建表
			handler.createHTable(tableName, tableInfor.getFamily());
		} catch (Exception e) {
			throw new Exception(e) ;
		}
		/**
		 * 2.循环遍历表数据,插入数据
		 */
		@SuppressWarnings("unused")
		int startIdx = getStart()  ;
		int endIdx = getEnd();
		long devide = getSize()/100 ;
		long loop = (getSize()%100==0? devide :devide+1) ;
		 if( logger.isDebugEnabled()){
			 logger.debug( tableName+" loop times:"+loop );
		 }
		 
		 setFilePath( acc.getFilePath() ); //设置文件的绝对路径
		 msAcc.setFile( null );
		//setOffset(2);
		try {
			for(int i =1 ;i< loop+1 ;i++){
				String filePath = this.getFilePath() ;
				List<Map<String,String>> rows =readFileACCESS(filePath, i) ;
			//	List<Map<String,String>> rows =getSomeRows( conn , tableName,i,tableInfor.getIndex() ) ;
				if( null == rows ) {
					continue ;
				}
				for(Map<String,String> row : rows){
					//write data
					insertOneRow(handler,tableInfor, row);
				}
				startIdx = endIdx ;
				endIdx = endIdx+ getOffset() ;
				
			}
		} catch (Exception e) {
			 logger.error("can't read data from access",e);
		}finally{
			Checker.gracefullyClose(rs);
			Checker.gracefullyClose(stmt);
			Checker.gracefullyClose(conn);
		}
		if( logger.isDebugEnabled()){
			 logger.debug( tableName+" loop over"  );
		 }
		return null;
	}
	
	private void insertOneRow(IHbandler handler,ITable tableInfor,Map<String,String> row) throws Exception{
		List<HbaseCell> columns = new ArrayList<HbaseCell>() ;
		String rowkey = row.get( tableInfor.getIndex() )==null?row.get( tableInfor.getIndex().toUpperCase() ):"" ; //数据库主键作为rowkey
		for(String key : row.keySet() ){
			//row,family,qualifier,value
			columns.add( HbaseUtil.getACell( rowkey , tableInfor.getFamily(), key, row.get(key) ) ) ;
		}
		//DEBUG
		//System.out.println("handler.handleRow:"+ JSONObject.toJSONString( columns.get(0).getRowName() ) );
		handler.handleRow(tableInfor.getTablenName(), tableInfor.getFamily(), columns);
	}

	public Connection newConnection(String url,Properties prop){
		try {
			return  DriverManager.getConnection(url, prop);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null ;
	}
	/**
	 * 这句sql是导致全表扫描，很费时
	 * TODO don't use this 
	 * @deprecated
	 */
	private String getSQL(String tableName,int currPage){
		if( currPage == 0 ){
			return null ;
		}
		String pk = "" ;
		int range = (getOffset()*(currPage-1)) ;
		String subSql = "" ;
		String id = "id" ;
		if( null != pk && !"".equals( pk ) ){
			id = pk ;
		}
		
		if( range == 0 ){
			subSql = "";
		}else{
			subSql = " where "+id+" not in (select top "+range+" "+id+" from "+tableName+" order by "+id+" ) " ;
		}
		return "select top "+getOffset()+" * from "+tableName+subSql+" order by "+id+" " ;
	}
	
	private  Connection conn; 
	private Statement stmt; 
    private ResultSet rs; 
	
	/**
	 * TODO : 读取文件access
	 * 
	 * @param filePath
	 * @return
	 * @throws ClassNotFoundException
	 */
	public List<Map<String,String>>  readFileACCESS(String filePath,int page) {
		//jdbc:odbc:Driver={Microsoft Access Driver (*.mdb, *.accdb)}; DBQ=
		List<Map<String,String>> maplist = new ArrayList<Map<String,String>>(); //一行数据
		try {
			String sql = getSQL("Dm_Mobile", page) ;
			if( null == sql ){
				return maplist;
			}
// 10 -20
			//rs = stmt.executeQuery("select * from (select top 10 id from (select top 20 id from Dm_Mobile order by id) t1 order by id desc) t2 order by id");
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
			Properties prop = new Properties();
			prop.put("charSet", "gb2312"); // 这里是解决中文乱码
			prop.put("user", "");
			prop.put("password", "");
			String url = "jdbc:odbc:driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=" + filePath; // 文件地址
			conn = newConnection(url, prop) ;
			stmt = (Statement) conn.createStatement();
			rs = stmt.executeQuery( sql ) ;
			ResultSetMetaData data = rs.getMetaData();

			while (rs.next()) {
				Map map = new HashMap();
				for (int i = 1; i <= data.getColumnCount(); i++) {
					String columnName = data.getColumnName(i); // 列名
					String columnValue = rs.getString(i);
					//Checker.print(columnName, columnValue);
					map.put(columnName, columnValue);
				}
				maplist.add(map);
				//System.out.println( map.get("ID")+">>>>>>"+JSONObject.toJSONString(map));
			}
		} catch (Exception e) {
			logger.error("access error",e);
			e.printStackTrace();
		}finally {
			Checker.gracefullyClose(rs);
			Checker.gracefullyClose(stmt);
			Checker.gracefullyClose(conn);
			
		}
		return maplist;
	}
	
	/**
	 * 根据SQL获取固定行数 
	 * currPage 第几页
	 * pk 主键
	 */
	private List<Map<String,String>> getSomeRows(Connection conn,String tableName,int currPage,String pk){
		if( currPage < 1 ){
			return null ;
		}
		List<Map<String,String>> maplist = new ArrayList<Map<String,String>>(); //一行数据
		String sql = getSQL(tableName,currPage,pk) ;
		Statement st = null ;
		ResultSet rs = null;
		//System.out.println( sql );
		try {
			Class.forName("sun.jdbc.odbc.JdbcOdbcDriver");
			st = (Statement) conn.createStatement() ;
			logger.debug( " Connection>"+conn );
			logger.info(tableName+" sql:"+ sql );
			//st.setQueryTimeout(1);
			rs = st.executeQuery(sql) ;
//			PreparedStatement ps = conn.prepareStatement(sql) ;//72e46ea8
//			rs = ps.executeQuery() ;
			logger.debug( " executeQuery>"+conn );
			
			ResultSetMetaData data = rs.getMetaData();

			while (rs.next()) {
				Map<String, String> map = new HashMap<String, String>();
				for (int i = 1; i <= data.getColumnCount(); i++) {
					String columnName = data.getColumnName(i); // 列名
					String columnValue = rs.getString(i);
					map.put(columnName, columnValue);
				}
				maplist.add(map);
			}
		} catch (Exception e) {
			logger.error("can't get data "+e.getMessage(),e);
		}finally {
			Checker.gracefullyClose(rs);
			Checker.gracefullyClose(st);
			Checker.gracefullyClose(conn);
		}
		return maplist;
	}
	

	public int getStart() {
		return start;
	}

	public int getOffset() {
		return offset;
	}

	public void setStart(int start) {
		this.start = start;
	}

	public void setOffset(int offset) {
		this.offset = offset;
	}

	public void setWhere(String where) {
		this.where = where;
	}

	public long getSize() {
		return size;
	}

	public void setSize(long size) {
		this.size = size;
	}

	public String getFilePath() {
		return filePath;
	}

	public void setFilePath(String filePath) {
		this.filePath = filePath;
	}
	
}