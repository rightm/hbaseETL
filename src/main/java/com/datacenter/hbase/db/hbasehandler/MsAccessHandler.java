package com.datacenter.hbase.db.hbasehandler;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import com.datacenter.hbase.Util.HbaseUtil;
import com.datacenter.hbase.adapter.HbaseAdapter;
import com.datacenter.hbase.adapter.HbaseCell;
import com.datacenter.hbase.adapter.HbaseClient;
import com.datacenter.hbase.db.htable.intf.ITable;

public class MsAccessHandler implements IHbandler{
	private Logger logger = Logger.getLogger( getClass() ) ;
	private HbaseAdapter hbase;
	private ITable tableInfor ;
	
	public MsAccessHandler(HbaseClient hClient ) {
		this.hbase =new HbaseAdapter( hClient ) ;
	}
	
	/**
	 * create a table in the Hbase
	 */
	public void createHTable(String tableName,String colFamily)throws Exception{
		try {
			getHbase().createTable( tableName, colFamily, false);
		} catch (Exception e) {
			logger.error("",e);
		}
	}
	/**
	 * 为每条数据都生成一个uuid，来标示
	 */
	private HbaseCell uuid(String tablename ,HbaseCell cell){
		HbaseCell cellUuid = new HbaseCell() ;
		cellUuid.setTableName( cell.getTableName() );
		cellUuid.setRowName( cell.getRowName() );
		cellUuid.setFamily( cell.getFamily() );
		cellUuid.setQualifier(  HbaseUtil.uuidName(tablename) ); // tablename+"_uuid" 
		cellUuid.setValue( HbaseUtil.uuidGen() );
		return cellUuid ;
	}
	
	private void insertDatas(String tableName,String colFamily,List<HbaseCell> columns,boolean repeated) throws Exception{
		List<Put> puts = new ArrayList<Put>() ;
		String rowkey = columns.get(0).getRowName() ;
		if( repeated ){
			rowkey = rowkey+":"+System.currentTimeMillis() ; //create a new row
			changeRowkey(columns, rowkey );
		} 
		
		boolean uuid = false ;
		
		for(HbaseCell cell : columns ){
			if( !uuid ){
				//产生uuid
				puts.add( HbaseUtil.putMe(rowkey, uuid(tableName,cell)) ) ;
				uuid = true ;
			}
			puts.add( HbaseUtil.putMe(rowkey, cell)) ; 
		}
		getHbase().insertTO(tableName, puts);
	}
	
	private boolean repeated(String tableName,String row,String family,String column,String auxKey) throws Exception{
		Result r0 = getHbase().selectRowKeyFamilyColumn(tableName, row, family, column) ;
		HbaseCell cell = HbaseUtil.getACell(r0) ;
		if( cell!=null && cell.getValue().equals( auxKey )){
			return true ;
		}
		return false ;
	}
	
	public void handleRow(String tableName,String colFamily,List<HbaseCell> columns) throws Exception {
		if( null == columns ) return ;
		String rowkey = columns.get(0).getRowName() ;
		boolean repeated = false ;
		
		if( getHtable().getAuxiliaryKey()!=null && getHbase().existRow(tableName, rowkey) ){
			String auxKey = getHtable().getAuxiliaryKey() ;
			 
			HbaseCell auxKeyCell = new HbaseCell();
			repeated = true ;
			
			for(HbaseCell cell : columns){
				if( cell.getQualifier().equals( auxKey) ){
					auxKeyCell = cell ;
					break ;
				}
			}
			//数据去重,假如不仅仅是rowKey相同，而且连auxiliary key也一样，那么说明是同一条数据，skip 
			if( repeated(tableName, auxKeyCell.getRowName(), auxKeyCell.getFamily(), auxKeyCell.getQualifier(), auxKey)){
				return ;
			}
		}
		//System.out.println(columns.size()+">>>"+ JSONObject.toJSONString( columns ) );
		insertDatas( tableName, colFamily,columns,repeated) ;
	}
	
	private void changeRowkey(List<HbaseCell> columns,String rowKey){
		for(HbaseCell cell:columns){
			cell.setRowName( rowKey );
		}
	}

	public HbaseAdapter getHbase() {
		return hbase;
	}

	public void setHbase(HbaseAdapter hbase) {
		this.hbase = hbase;
	}

	public ITable getHtable() {
		return getTableInfor();
	}

	public ITable getTableInfor() {
		return tableInfor;
	}

	public void setTableInfor(ITable tableInfor) {
		this.tableInfor = tableInfor;
	}

	public void setHtable(ITable htable) {
		setTableInfor(htable);;
	}

}
