package com.datacenter.hbase.db.htable;

import java.util.Map;

import com.datacenter.hbase.db.htable.intf.IAccessTable;

/**
 * 手机归属地信息 
 */
public class DmMobile implements IAccessTable {
	
	public String getIndex() {
		//将主键作为hbase中的rowkey
		return "id";
	}

	public String getTablenName() {
		return "DM_MOBILE" ;
	}
	//@deprecated
	public String getWhere(String ...args) {
		String start = args[0] ;
		String end = args[1] ;
		return  "where ID > "+start+" and ID < "+end ; 
	}

	public String getFamily() {
		return "CF";
	}

	public String getAuxiliaryKey() {
		return null;
	}

	public String getPrimaryKey() {
		return "id";
	}

	public String getSQL() {
		return null;
	}

	public Map<String, Object> getColumnTypeMap() {
		// TODO Auto-generated method stub
		return null;
	}

	public boolean isIgnored(String key) {
		// TODO Auto-generated method stub
		return false;
	}
}
