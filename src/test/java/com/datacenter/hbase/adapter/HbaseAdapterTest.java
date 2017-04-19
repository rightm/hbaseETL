package com.datacenter.hbase.adapter;


import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result ;
import org.apache.hadoop.hbase.util.Bytes;

import com.datacenter.hbase.Util.HbaseUtil;  
/**
 * CRUD
 * @author Administrator
 *
 */
public class HbaseAdapterTest {
	private static HbaseClient client ;
	private static String tableName ;
	private static Connection conn = null ;
	private static HbaseAdapter adapter ;
	
	public static void main(String[] args) {
		//rowSelect() ;
		main0();
	}
	
	public static void batchQuery(){
		
	}
	
	public static void rowSelectRegular( ) {
		String row= "6223021" ;  //2846
		String family ="cf"; //cf:card_bank
		String column = "card_bank" ;
		tableName = "" ;
		try {
			
			Result r0 = new HbaseAdapter( client ).selectRowKeyFamilyColumn(tableName, row, family, column) ;
			System.out.println("cell exist?"+r0.getExists()  );
			HbaseCell cell = HbaseUtil.getACell(r0) ;
			if( cell != null ){
				System.out.println( cell.getQualifier()+"="+cell.getValue() );
			}
			System.out.println( "no cell");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void rowSelect( ) {
		String row= "6223021" ;  //2846
		String family ="cf"; //cf:card_bank
		String column = "card_bank" ;
		tableName = "" ;
		try {
			
			Result r0 = new HbaseAdapter( client ).selectRowKeyFamilyColumn(tableName, row, family, column) ;
			System.out.println("cell exist?"+r0.getExists()  );
			HbaseCell cell = HbaseUtil.getACell(r0) ;
			if( cell != null ){
				System.out.println( cell.getQualifier()+"="+cell.getValue() );
			}
			System.out.println( "no cell");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main0(){
		try {//["BaseSite", "Dm_Mobile", "IPAddressInfo", "PosBankCardBIN"]
			String row= "慈溪-跨海大桥2" ;  //2846
			Admin admin = conn.getAdmin() ;
			tableName = "POSBANKCARDBIN";//"PosBankCardBIN" ; 
			
			if( !admin.tableExists(TableName.valueOf(tableName)) ){
				System.err.println("no table exists");
				throw new Exception() ;
			}
			
			List<Get> list = new ArrayList<Get>() ;
			Get get = new Get(Bytes.toBytes( "622188" )) ;
			Get get1 = new Get(Bytes.toBytes( "955100" )) ;
			Get get2 = new Get(Bytes.toBytes( "621095" )) ;
			Get get3 = new Get(Bytes.toBytes( "620062" )) ;
			
			list.add(get) ;
			list.add(get1) ;
			list.add(get2) ;
			list.add(get3) ;
			//Result r0 = new HbaseAdapter(HbaseClient.build()).getRows(tableName , list).get(0)  ;
			List<Result> listR =new HbaseAdapter(HbaseClient.build()).getRows(tableName , list) ;
			for(Result r0:listR ){
				printR(r0);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	static {
		client = HbaseClient.build() ;
		tableName = "PosBankCardBINData" ;
		adapter = new HbaseAdapter(client) ;
		try {
			conn = client.openConnection() ;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static void gg(Result result){
		 List<KeyValue> list = result.list();   
	     for(final KeyValue kv:list){  
	        // System.out.println("value: "+ kv+ " str: "+Bytes.toString(kv.getValue()));  
	         System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",   
	                 Bytes.toString(kv.getRow()),   
	                 Bytes.toString(kv.getFamily()),   
	                 Bytes.toString(kv.getQualifier()),   
	                 Bytes.toString(kv.getValue()),  
	                 kv.getTimestamp()));       
	     }  
	}
	
	public static void printR(Result r){
		System.out.println( ">Get columns : "+ r.size() );  
		System.out.println( ">row:"+Bytes.toString(r.getRow()));
		for(Cell cell: r.listCells() ){
			System.out.println( Bytes.toString(cell.getQualifier()) +":"+Bytes.toString(cell.getValue() ) );
		}
	}
}