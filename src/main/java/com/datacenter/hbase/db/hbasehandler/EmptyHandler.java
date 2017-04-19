package com.datacenter.hbase.db.hbasehandler;

import java.util.List;

import com.datacenter.hbase.adapter.HbaseCell;
import com.datacenter.hbase.db.htable.intf.ITable;

public class EmptyHandler implements IHbandler {

	public void createHTable(String tableName, String colFamily) {
		// TODO Auto-generated method stub
		
	}

	public void handleRow(String tableName, String colFamily, List<HbaseCell> columns) throws Exception {
		// TODO Auto-generated method stub
		
	}

	public ITable getHtable() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setHtable(ITable htable) {
		// TODO Auto-generated method stub
	}

}
