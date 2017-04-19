package com.datacenter.hbase.db.hbasehandler;

import java.util.List;

import com.datacenter.hbase.Util.HbaseUtil;
import com.datacenter.hbase.adapter.HbaseCell;
import com.datacenter.hbase.db.htable.intf.ITable;
import com.datacenter.hbase.phoenix.PhoenixBaseDao;
import com.datacenter.hbase.phoenix.PhoenixClient;
import com.datacenter.hbase.phoenix.PhoenixConnectionPoolFactory;

public class PhoenixHandler implements IHbandler{
	private ITable htable ;
	private PhoenixBaseDao dao ;
	
	public PhoenixHandler() {
		PhoenixConnectionPoolFactory fac = new PhoenixConnectionPoolFactory() ;
		PhoenixClient client = PhoenixClient.build(fac) ;
		dao= new PhoenixBaseDao(client) ;
	}
	
	public void createHTable(String tableName, String colFamily) throws Exception {
		 //Create table manually 手动创建表
		//在Phoenix客户端上创建表，客户端会把字段自动转为大写
	}

	public void handleRow(String tableName, String colFamily, List<HbaseCell> columns) throws Exception {
		//columns 从excel读取的一列数据，空值的cell填充""字符串，所以能够保证所有列都有数据
		ITable tab = getHtable() ;
		String sql = HbaseUtil.sqlGen(columns, tab) ;
		dao.insert(sql) ;
	}

	public ITable getHtable() {
		return this.htable;
	}

	public void setHtable(ITable htable) {
		 this.htable = htable ;
	}

	public PhoenixBaseDao getDao() {
		return dao;
	}

	public void setDao(PhoenixBaseDao dao) {
		this.dao = dao;
	}

}
