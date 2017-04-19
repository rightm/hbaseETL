package com.datacenter.hbase.db;

import java.util.List;
import java.util.Map;

public interface AccessorOperation {
	public <T> List<T> acccess(DbAccessor acc,Map<String, Object> parameter) throws Exception;
}	
