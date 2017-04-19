package com.datacenter.hbase.adapter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.Cell ;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.JSONObject;
import com.datacenter.hbase.Util.HbaseUtil;

// Class that has nothing but a main.
// Does a Put, Get and a Scan against an hbase table.
public class MyLittleHBaseClientTest {
	public static void main(String[] args) throws IOException {
		final Connection conn = getConn() ;
		Table table = null ;
		try {
			
			//list(conn,1,"IPADDRESSINFO");
			table = conn.getTable(TableName.valueOf( "IPADDRESSINFO".getBytes() )) ;
			get(  table ,"16777216" );
			
		} catch (Exception e) {
			e.printStackTrace();
		}finally {
			HbaseUtil.closeHTable(table);
			HbaseUtil.closeHConn(conn);
		}
	}
	public static Connection getConn(){
		Configuration config = HBaseConfiguration.create();//config.addResource("hbase/hbase-site.xml");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum", "192.168.0.130");
		try {
			return ConnectionFactory.createConnection(config);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null ;
	}
	
	public static void list(final Connection connRef,final int limit,final String matchTab){
		try {
			listTbs(connRef.getAdmin(),limit, new Callbk() {
				public void callBack(TableName tn) throws IOException {
					int line = limit ;
					if( null!=matchTab && !matchTab.toUpperCase().equals( tn.getNameAsString() )){
						return ;//skip other table
					}
//					if( null!=matchTab && !matchTab.equals( tn.getNameAsString() )){
//						return ;//skip other table
//					}
					tableOperation(connRef,tn.getNameAsString(),line,true);
//				System.err.println("\n-------raw data:");
					//tableOperation(connRef,tn.getNameAsString(),false);
				}
			});
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static void listTbs(Admin admin,int limit,Callbk cb ) throws IOException{
		if ( null == admin ) {
			return ;
		}
		for( TableName tn : admin.listTableNames() ){
			System.out.println("table:"+ tn.getNameAsString() );
			if( null != cb ){
				cb.callBack(tn);
			}
		}
	}
	
	private static void tableOperation(Connection conn,String tableName,int line,boolean cell) throws IOException{
		Table table = conn.getTable(TableName.valueOf( tableName ));
		try {
			//get(table) ;
			 scan(table,cell,line) ;
		} finally {
			if (table != null)
				table.close();
		}
	}
	
	private static void printR(Result r){
		System.out.println( "Get columns : "+ r.size() );  
		for(Cell cell: r.listCells() ){
			System.out.println( Bytes.toString(cell.getQualifier() ) +":"+Bytes.toString(cell.getValue()));
		}
	}
	
	public static void get(Table table,String rowKey  ) throws IOException{
		Get g = new Get(Bytes.toBytes( rowKey));
		Result r = table.get(g);
		System.out.println( "Get columns : "+ r.size() );  
		List<Get> list = new ArrayList<Get>() ;
		list.add( g ) ;
		try {
			Result r0 = new HbaseAdapter(HbaseClient.build()).getRows(table.getName().getNameAsString(), list).get(0)  ;
			printR(r0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	@SuppressWarnings("deprecation")
	public static void scan(Table table,boolean listCell,int lines) throws IOException{
		Scan s = new Scan();
		//s.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
		ResultScanner scanner = table.getScanner(s);
		System.out.println("scanner get");
		try {
			int i = 0 ;
			for (Result rr = scanner.next(); rr != null; rr = scanner.next()) {
				if( i++ == lines ){
					break ;
				}
				 if(listCell){
					 if( table.getName().getNameAsString().equals("TEST") ){
						 System.out.print("row:"+  Bytes.toInt(rr.getRow())+" " ) ;
					 }else{
						 System.out.print("row:"+ Bytes.toString(rr.getRow())+" " ) ;
					 }
					 for(Cell cell: rr.listCells()){
//						 cell.getFamily()
						System.out.print( Bytes.toString(cell.getFamily())+":"+Bytes.toString(cell.getQualifier())
								+":"+Bytes.toString(cell.getValue()) +" ");
					 }
				 }else{
					 System.out.println("Found row: " + rr);
				 }
				 System.out.println();
			}
		}catch(Exception e){
			e.printStackTrace();
		} finally {
			scanner.close();
		}
	}
	
	public static void putData(Table table,String tableName) throws IOException{
		Put p = new Put(Bytes.toBytes( tableName ));
		p.add(Bytes.toBytes("myLittleFamily"), Bytes.toBytes("someQualifier"), Bytes.toBytes("Some Value"));
		table.put(p);
	}
	
}

interface Callbk{
	void callBack(TableName tn) throws IOException ;
}