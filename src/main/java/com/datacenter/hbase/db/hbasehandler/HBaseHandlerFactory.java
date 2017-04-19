package com.datacenter.hbase.db.hbasehandler;

public class HBaseHandlerFactory {
	public IHbandler getHandler(){
		return new PhoenixHandler();
	}
}
