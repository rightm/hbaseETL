package com.datacenter.hbase.db.htable.intf;

import java.util.Map;

public interface ITable {
	public static final int TYPE_STRING = 0 ; //string
	public static final int TYPE_INT = 1; //int 
	
	/**
	 * 作为hbase中的rowKey的列的名称，获取指定的主键 ,比如excel中哪一列作为主键
	 */
	public String getIndex();
	/**
	 * 获取本类对应的表名称 ，统一大写
	 */
	public String getTablenName() ;
	/**
	 * 指定column family 
	 * @return
	 */
	public String getFamily() ;
	/**
	 * 获取遍历本表的where条件
	 * @deprecated
	 */
	public String getWhere(String ...args);
	/**
	 * hbase中辅助性的列，例如帮助去重，搜索等
	 * @return
	 */
	public String getAuxiliaryKey() ;
	/**
	 * 列类型映射，即某一列对应的数据类型是什么
	 */
	public Map<String,Object> getColumnTypeMap() ;
	/**
	 * 过滤字段，哪些字段是不需要导入的 
	 */
	public boolean isIgnored(String key);
	
}
