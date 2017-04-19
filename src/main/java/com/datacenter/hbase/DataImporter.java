package com.datacenter.hbase;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.datacenter.hbase.Util.Checker;
import com.datacenter.hbase.Util.HbaseUtil;
import com.datacenter.hbase.Util.PropertiesLoader;
import com.datacenter.hbase.adapter.HbaseClient;
import com.datacenter.hbase.comm.FileScanWorker;
import com.datacenter.hbase.comm.ICallBack;
import com.datacenter.hbase.db.AccessorOpFactory;
import com.datacenter.hbase.db.DbAccessorFactory;

/**
 * 1、创建一个表信息，实现ITable(在db/htable package下)
 * 2、将文件存放在配置文件中target.dir配置的目录下（支持目录）
 * @author Administrator
 *
 */
public class DataImporter {
	private Logger logger = Logger.getLogger(getClass()) ;
	private String sourceFolder ; 
	private BlockingQueue<File> resources ;
	private ExecutorService threadPool;
	private AccessorOpFactory opfac  ;
	private boolean includeMsAccess = false ;
	
	public DataImporter( ){
		this(false) ;
	}
	
	public DataImporter( boolean includeMsAccess) {
		this.includeMsAccess = includeMsAccess ;
		File[] files;
		try {
			files = getFiles();
			logger.debug(">scan data dir<"+sourceFolder+"> get data files :"+files.length );
			resources = new ArrayBlockingQueue<File>(files.length) ;
			addFile(resources, files);
			setOpfac( new AccessorOpFactory() );
		} catch (Exception e) {
			logger.error( "no files in target.dir="+sourceFolder );
		}
	}
	
	/**
	 * main method
	 */
	public void importData(){
		int poolSize = 3 ;
		try {
			File[] files = getFiles() ;
			HbaseClient hClient =HbaseClient.build() ;
			if( null != files ){
				if(files.length > 20){
					poolSize = 5 ;
				}
				poolSize = 1 ; //还是慢慢处理把，不然直接oom了
				threadPool = HbaseUtil.getPool(this.getClass().getName(), poolSize) ;
				for(int i=0;i<poolSize;i++){
					threadPool.execute( new FileScanWorker(this, new DbAccessorFactory(),hClient) );
				}
				logger.info("thread pool start consume the resources");
			}else{
				logger.warn("no files under "+getDataFolder() );
			}
		} catch (Exception e) {
			logger.error("can not import data",e);
		}finally {
			if( null != threadPool ){
				threadPool.shutdown();
			}
		}
	}
	
	/**
	 * add files into the queue recursively
	 * @param resources
	 * @param file
	 */
	private void addFile(final BlockingQueue<File> resources ,File ...file){
		HbaseUtil.list( file, new ICallBack() {
			public void callback(Object arg) {
				File ff = (File) arg ;
				if( null == ff ) return ;
				if( !includeMsAccess && Checker.isAccess(ff.getName()) ){
					return ;
				}
				if( !ff.isDirectory() ){
					resources.add(ff) ;
				}else{
					addFile(resources, ff);
				}
			}
		});
	}
	
	/**
	 * get files from target folder which set in the configuration file
	 * @return
	 */
	public String getDataFolder() throws Exception{
		if( null == sourceFolder ){
			sourceFolder = PropertiesLoader.get("target.dir", null) ;
			if( StringUtils.isNotEmpty(sourceFolder) && Checker.folderExists(sourceFolder)  ){
				logger.info("Import data from "+sourceFolder );
			}else{
				throw new IllegalArgumentException("no files in target.dir="+sourceFolder);
			}
		}
		return sourceFolder ;
	}
	/**
	 * get all the DB data file
	 * @return
	 * @throws Exception
	 */
	public File[] getFiles() throws Exception{
		String source = getDataFolder() ;
		if( null == source ){
			return null ;
		}
		File dir = new File(source) ;
		return dir.listFiles() ;
	}
	/**
	 * get a file from the queue and consume
	 * @return
	 */
	public File consume(){
		if( null != getResources() ){
			return getResources().poll() ;
		}
		return null ;
	}
	/**
	 * has consumed all the db data files
	 */
	public boolean isFinished(){
		return null != resources && getResources().size() == 0 ;
	}

	public BlockingQueue<File> getResources() {
		return resources;
	}

	public void setResources(BlockingQueue<File> resources) {
		this.resources = resources;
	}

	public AccessorOpFactory getOpfac() {
		return opfac;
	}

	public void setOpfac(AccessorOpFactory opfac) {
		this.opfac = opfac;
	}
	
}
