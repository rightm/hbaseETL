package com.datacenter.hbase.db;

import java.util.HashMap;
import java.util.Map;

import com.datacenter.hbase.Util.RefectUtil;
import com.datacenter.hbase.db.access.DefaultMsAccessOperation;
import com.datacenter.hbase.db.excel.DefaultMsExcelXMLOperation;


public class AccessorOpFactory {
	private String keyPrefix = "accessop" ;//
	//文件映射 <-> 行为<> ,实现AccessorOperation接口
	//比如 key = a.mdb ,value = com.datacenter.hbase.xxx.AOperation
	private Map<String, String> operationMap = new HashMap<String, String>() ;
	
	public AccessorOpFactory() {
		//TODO 
		//解析配置文件 所有格式为accessop.xx.xx的都作为AccessorOperation的实现类
		//遍历配置文件  
		//例如如下配置  
		// accessop.fileName=com.datacenter.hbase.Util.RefectUtil
		// setOpClazz(fileName,com.datacenter.hbase.Util.RefectUtil) ;
	}
	
	@SuppressWarnings("unused")
	private void setOpClazz(String fileName,String clazz){
		operationMap.put(fileName ,clazz ) ;
	}
	
	private String getOperationClazz(String key){
		return operationMap.get( key ) ;
	}
	
	/**
	 * 假如有更多的表，格式不同需要处理，那么请实现AccessorOperation这个接口来提供不同格式数据处理
	 * @param acc
	 * @param arg  maybe file name or anythings 
	 * @return
	 */
	public AccessorOperation getOperation(DbAccessor acc,String ...arg){
		String type = acc.getDbType() ;
		 
		if( DBConstants.MS_ACCESS.equals(type)){
			return new DefaultMsAccessOperation(type, 0, 100) ;
		}
		if( DBConstants.MS_EXCEL.equals(type)){
			return new DefaultMsExcelXMLOperation() ;
			//return new MsExcel.DefaultMsExcelOperation() ;
		}
		
		String fileName = arg[0] ;
		String operationClazz =getOperationClazz(fileName) ;
		return (AccessorOperation) RefectUtil.invoke(operationClazz) ;
	}
}
