package com.datacenter.hbase.db.htable.intf;

/**
 * access  表数据
 * @author Administrator
 *
 */
public interface IAccessTable extends ITable {
	/**
	 * 主键 列名称
	 * @return
	 */
	public String getPrimaryKey() ;
	/**
	 * 不同表根据不同的主键，不同的分页方式来遍历数据
	 * @return
	 */
	public String getSQL();
	
}
