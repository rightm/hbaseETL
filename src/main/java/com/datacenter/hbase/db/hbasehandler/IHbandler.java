package com.datacenter.hbase.db.hbasehandler;

import java.util.List;

import com.datacenter.hbase.adapter.HbaseCell;
import com.datacenter.hbase.db.htable.intf.ITable;

/**
 * 在HBASE 建立表，并且插入数据
 * @author Administrator
 */
public interface IHbandler {
	public void createHTable(String tableName,String colFamily) throws Exception  ;
	public void handleRow(String tableName,String colFamily,List<HbaseCell> columns) throws Exception;
	public ITable getHtable();
	public void setHtable(ITable htable);
}
