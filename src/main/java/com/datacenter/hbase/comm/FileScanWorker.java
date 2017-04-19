package com.datacenter.hbase.comm;

import java.io.File;

import org.apache.log4j.Logger;

import com.datacenter.hbase.DataImporter;
import com.datacenter.hbase.adapter.HbaseClient;
import com.datacenter.hbase.db.DbAccessor;
import com.datacenter.hbase.db.DbAccessorFactory;

public class FileScanWorker implements Runnable{
	private DbAccessorFactory accFac;
	private DataImporter dimp ;
	private HbaseClient hClient ;
	private Logger logger = Logger.getLogger(getClass()) ;
	
	public FileScanWorker(DataImporter dimp,DbAccessorFactory accFac,HbaseClient hClient) throws Exception {
		this.accFac = accFac ;
		if( this.accFac == null ){
			this.accFac = new DbAccessorFactory() ;
		}
		this.dimp = dimp ;
		this.hClient = hClient ;
	}
	
	public void run() {
		logger.debug("one job start");
		if( null == this.dimp ){
			logger.warn("no DataImporter specified in the FileScanWorker");
			return ;
		}
		 while( !this.dimp.isFinished()){
			 File file = this.dimp.consume() ;
			 if( file == null ){
				 continue ;
			 }
			 if( logger.isDebugEnabled() ){
				 logger.debug(">>>consuming "+file.getName() );
			 }
			 //一个文件对应一个访问器
			 DbAccessor acc = accFac.getAccessor(file,this.hClient) ;
			 try {
				acc.access(  this.dimp.getOpfac()  );
			} catch (Exception e) {
				logger.error("access error",e);
				break ;
			}
		 }
	}

}
