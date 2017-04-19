package com.datacenter.hbase.Util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.datacenter.hbase.adapter.HbaseCell;
import com.datacenter.hbase.comm.ICallBack;
import com.datacenter.hbase.comm.NamedThreadFactory;
import com.datacenter.hbase.db.htable.PosBankCardBINData;
import com.datacenter.hbase.db.htable.intf.IExcelTable;
import com.datacenter.hbase.db.htable.intf.ITable;

public final class HbaseUtil {
	private HbaseUtil() { }
	
	public static void main(String[] args) {
		ITable tab = new PosBankCardBINData() ;
		List<HbaseCell> cols = new ArrayList<HbaseCell>() ;
		
//		typeMap.put("card_bank", ITable.TYPE_STRING ) ;
//		typeMap.put("card_name", ITable.TYPE_STRING  ) ;
//		typeMap.put("card_num_len", ITable.TYPE_INT ) ;
//		typeMap.put("card_num_style", ITable.TYPE_STRING  ) ;
//		typeMap.put("card_six_bits", ITable.TYPE_INT  ) ; //主要用于区分数字还是字符
//		typeMap.put("card_type", ITable.TYPE_STRING  ) ;
		
		
		
		HbaseCell cell1 = new HbaseCell() ;
		cell1.setQualifier("card_bank");
		cell1.setValue("11111");
		HbaseCell cell2 = new HbaseCell() ;
		cell2.setQualifier("card_name");
		cell2.setValue("China bank");
		HbaseCell cell3 = new HbaseCell() ;
		cell3.setQualifier("card_num_len");
		cell3.setValue("19");
		HbaseCell cell4 = new HbaseCell() ;
		cell4.setQualifier("card_type");
		cell4.setValue("xingyongka");
		cols.add(cell1);
		cols.add(cell2);
		cols.add(cell3);
		cols.add(cell4);
		
		System.out.println( sqlGen( cols, tab));
		
	}
	
	public static String uuidName(String  tablename ){
		return  tablename+"_uuid" ;
	}
	
	public static String sqlGen( List<HbaseCell> columns,ITable tab){
		//upsert into test values(4,'bb')
		String tableName  = tab.getTablenName() ;
		StringBuilder sql = new StringBuilder("upsert into ").append(tableName);
		StringBuilder keys =new StringBuilder();
		StringBuilder values =new StringBuilder() ;
		boolean isFirst = true ;
		String uuid = uuidGen() ;
		
		keys.append("uuid");
		values.append("'").append(uuid).append("'") ;
		for(HbaseCell cell :columns){
			if( cell.getValue() != null && !"".equals( cell.getValue().trim() )){
				keys.append(",");
				values.append(",") ;
				String key = cell.getQualifier()  ;
				String value = cell.getValue() ;
				Object _t =  tab.getColumnTypeMap().get(key)  ;
				if( _t == null ) _t = ITable.TYPE_STRING;
				int type =   (Integer)_t;
				keys.append( key ) ;
				if( type == ITable.TYPE_INT){
					values.append( value ) ;
				}else if( type == ITable.TYPE_STRING){
					values.append( "'").append( value).append("'") ;
				}
			}
		}
		
		sql.append("(").append(keys).append(") ") ;
		sql.append("values(").append( values).append(")") ;
		return sql.toString() ;
	}
	
	/**
	 * 生成uuid
	 * @return
	 */
	public static String uuidGen(){
		return UUID.randomUUID().toString() ;
	}
	
	public static byte []bytes(String val) throws UnsupportedEncodingException{
		if ( null != val) {
			return Bytes.toBytes( val ) ;
			//return val.getBytes("UTF-8") ;
		}
		return null ;
	}
	
	public static Table getHTable(Connection conn ,String tableName) throws IllegalArgumentException, IOException{
		Checker.checkNull(conn);
		Checker.checkNull(tableName);
		
		return conn.getTable(TableName.valueOf( Bytes.toBytes( tableName) )) ;
	}
	
	public static void tableCheck(Connection conn, String tableName ) throws Exception{
		Checker.checkNull(conn);
		Checker.checkNull(tableName);
		if( !conn.getAdmin().tableExists(TableName.valueOf(tableName)) ){
			throw new Exception("no table exists:"+tableName) ;
		}
	}
	
	public static ExecutorService getPool(String name,int size){
		return Executors.newFixedThreadPool(size, new NamedThreadFactory(name)) ;
	}
	
	public static Put putMe(String rowkey,HbaseCell cell){
		Put put = new Put(Bytes.toBytes(rowkey));
		long ts =System.currentTimeMillis() ;
		if( cell.getValue() == null ){
			cell.setValue("");
		}
		put.addColumn( Bytes.toBytes(cell.getFamily() ), Bytes.toBytes(cell.getQualifier().toUpperCase() ),ts, Bytes.toBytes(cell.getValue()) );
		//put.addColumn(family, qualifier, ts, value)
		return put;
	}
	
	/**
	 * set value in sequence with row,family,qualifier,value
	 */
	public static HbaseCell getACell(String a,String b,String  c,String d){
		HbaseCell cell = new HbaseCell() ;
		cell.setRowName(a);
		cell.setFamily(b);
		cell.setQualifier(c);
		cell.setValue(d);
		return cell ;
	}
	
	public List<HbaseCell> getCells(Result r){
		List<HbaseCell> list = new ArrayList<HbaseCell>() ;
        KeyValue[] kv = r.raw();
        for (int i = 0; i < kv.length; i++) {
            String rowkey = Bytes.toString( kv[i].getRow())  ;
            String family = Bytes.toString( kv[i].getFamily()) ;
            String qualifier = Bytes.toString( kv[i].getQualifier()) ;
            String value = Bytes.toString( kv[i].getValue() ) ;
            
            HbaseCell cell = getACell( rowkey , family , qualifier , value ) ;
            list.add( cell ) ;
        }
        return list ;
	}
	
	public static HbaseCell getACell(Result r){
		if( r.size() != 1 ) return null;  
		String rowKey = Bytes.toString(r.getRow()) ;
		Cell cell = r.listCells().get(0) ;
		String family = Bytes.toString( cell.getFamily() ) ; 
		return getACell( rowKey, family, Bytes.toString(cell.getQualifier()),  Bytes.toString(cell.getValue()) ) ;
	}
	
	public static List<Object> getAList(int size){
		 List<Object> cellList = new ArrayList<Object>( size);
		 for(int i=0;i< size;i++){
			 cellList.add("") ;
		 }
		 return cellList ;
	}
	
	/**
	 * set value in sequence with row,family,qualifier,value
	 */
	public static List<HbaseCell> getACell( IExcelTable table,List<Object> d){
		List<Object> columns = table.getExcelColumns() ;
		List<HbaseCell> cells = new ArrayList<HbaseCell>() ;
		String rowKey = (String)d.get( table.getRowKeyIndex() ) ;
		for(int i = 0 ; i< d.size() ;i++ ){
			HbaseCell cell = new HbaseCell() ;
			String qualifier = getQualifier(table, (String)columns.get(i) )  ;
//			if( table.isIgnored(qualifier)||table.isIgnored(qualifier.toUpperCase() ) ){
//				//skip the column that need ignored
//				continue ;
//			}
			cell.setTableName( table.getTablenName());
			cell.setFamily( table.getFamily());
			cell.setRowName( rowKey );
			cell.setQualifier( qualifier );
			cell.setValue( (String)d.get(i ));
			cells.add( cell ) ;
		}
		return cells ;
	}
	
	private static String getQualifier(IExcelTable table,String value){
		if( null == table.getFamilyNameMap() ){
			//没有做映射，那么按照原来excel文件中的字段设置
			return value ;
		}
		String valueFormated = table.getFamilyNameMap().get( value ) ;
		//System.out.println( value +"==="+valueFormated);
		return valueFormated == null ? value:valueFormated;
	}
	
	public static String formatStr(List<HbaseCell> columns){
		StringBuilder builder =new StringBuilder() ;
		boolean first = true ;
		for(HbaseCell cell :columns ){
			if( first ){
				first = false ;
			}else{
				builder.append(",|") ;
			}
			//builder.append(cell.getRowName()+">"+cell.getQualifier()+":"+cell.getValue() );
			builder.append(  cell.getQualifier()+"="+cell.getValue() );
		}
		return builder.toString() ;
	}
	
	public static void list(Object[] arr,ICallBack callback){
		if( null != arr ){
			for(Object obj :arr){
				if( null != callback){
					callback.callback( obj );
				}
			}
		}
	}
	
	public static void closeHTable(Table table){
		if( null != table ){
			try {
				table.close();
			} catch (Exception e) {
			}
		}
	}
	
	public static void closeHConn(Connection conn){
		if( null != conn && !conn.isClosed() ){
			try {
				conn.close();
			} catch (IOException e) {
				 
			}
		}
	}
}
