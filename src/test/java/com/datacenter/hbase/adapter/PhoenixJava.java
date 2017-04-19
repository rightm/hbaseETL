package com.datacenter.hbase.adapter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import com.datacenter.hbase.Util.Checker;
import com.datacenter.hbase.phoenix.PhoenixBaseDao;
import com.datacenter.hbase.phoenix.PhoenixClient;
import com.datacenter.hbase.phoenix.PhoenixConnectionPoolFactory;
/**
 * 
 * @author Administrator
 * ref:http://www.jianshu.com/p/d862337247b1
 */
public class PhoenixJava {
	
	public static void main(String[] args) {
		String sql = "select * from POSBANKCARDBIN where card_bank like '宁波银行 (64083300)%'";
		test( sql ); 
		//testInsert() ;
//		PhoenixConnectionPoolFactory fac = new PhoenixConnectionPoolFactory() ;
//		PhoenixClient client = PhoenixClient.build(fac) ;
//		PhoenixBaseDao dao= new PhoenixBaseDao(client) ;
//		//upsert into test values (2,'World!')
//		String sql = "upsert into test(MYKEY) values(6)";
//		System.out.println("inserted ? "+ dao.insert(sql));
	}
	
	public static void split(int i,int columnSize,Object cell){
		if( i != 1 ){
			System.out.print("\t");
		}
		System.out.print( cell );
		if( i == columnSize ){
			System.out.println();
		}
	}
	
	public static void meta( ResultSetMetaData meta){
		try {
			int size= meta.getColumnCount() ;
			for(int i=1;i<=size;i++){
				split(i, size,meta.getColumnName(i) );
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void testInsert() {
	    Connection conn = null;
	    Statement statement = null ;
	    ResultSet rs = null ;
	    String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
	    String url = "jdbc:phoenix:192.168.0.130:2181,192.168.0.135:2181";
	 
	    try {
	        Class.forName(driver);
	    } catch (ClassNotFoundException e) {
	        e.printStackTrace();
	    }
	 
	    if (conn == null) {
	        try {
	        	conn = DriverManager.getConnection(url);
	            statement = conn.createStatement();
	           // String sql = "select count(1) as num from STOCK_SYMBOL";
	            String sql = "upsert INTO STOCK_SYMBOL(SYMBOL,COMPANY) VALUES('a','b')";
	            boolean flag = statement.execute(sql) ;
	            System.out.println( "success?"+flag);
	        } catch ( Exception e) {
	            e.printStackTrace();
	        }finally{
	        	Checker.gracefullyClose(rs);
	        	Checker.gracefullyClose(statement);
	        	Checker.gracefullyClose(conn);
	        }
	    }
	}
	
	public static void test(String sql) {
	    Connection conn = null;
	    Statement statement = null ;
	    ResultSet rs = null ;
	    String driver = "org.apache.phoenix.jdbc.PhoenixDriver";
	    String url = "jdbc:phoenix:192.168.0.130:2181";
	 
	    try {
	        Class.forName(driver);
	    } catch (ClassNotFoundException e) {
	        e.printStackTrace();
	    }
	 
	    if (conn == null) {
	        try {
	        	conn = DriverManager.getConnection(url);
	            statement = conn.createStatement();
	           // String sql = "select count(1) as num from STOCK_SYMBOL";
	            
	            long time = System.currentTimeMillis();
	            rs = statement.executeQuery(sql);
	            ResultSetMetaData meta = rs.getMetaData() ;
	            int columns = meta.getColumnCount() ;
	            meta(meta);
	            while (rs.next()) {
	            	//int count = rs.getInt("num");
	            	for(int i=1 ;i<= columns ;i++){
	            		Object cell = rs.getObject( i) ;
	            		
	            		split(i, columns,cell);
	            	}
	            	break ;
	            }

	        } catch (SQLException e) {
	            e.printStackTrace();
	        }finally{
	        	Checker.gracefullyClose(rs);
	        	Checker.gracefullyClose(statement);
	        	Checker.gracefullyClose(conn);
	        }
	    }
	}
}
