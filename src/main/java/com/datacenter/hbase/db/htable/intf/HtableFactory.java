package com.datacenter.hbase.db.htable.intf;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import com.alibaba.fastjson.JSONObject;
import com.datacenter.hbase.Util.RefectUtil;
import com.datacenter.hbase.db.htable.DmMobile;
import com.datacenter.hbase.db.htable.PosBankCardBINData;


public class HtableFactory {
	public static String packageMe = "com.datacenter.hbase.db.htable" ;
	private static List<Object> objs ;
	
	static{
		try {
			objs = getObject() ;
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static ITable getItable(String tableName){
		if( new DmMobile().getTablenName().equals( tableName)){
			return new DmMobile() ;
		}
//		if( new PosYinLinaBlackList().getExcelFileName().equals( tableName)){
//			return new PosYinLinaBlackList() ;
//		}
		if( new PosBankCardBINData().getExcelFileName().equals( tableName)){
			return new PosBankCardBINData() ;
		}
		//发射生成实例
		if( null != objs ){
			for(Object itable: objs){
				IAccessTable access = null ;
				IExcelTable excel = null ;
				
				if( itable instanceof IAccessTable ){
					access = (IAccessTable) itable;
					if( access.getTablenName().equals( tableName) ){
						try {
							return access.getClass().newInstance() ;
						} catch ( Exception e) {
							System.out.println( e.getMessage() );
						}
					}
				}else{
					excel = (IExcelTable) itable ;
					if( excel.getExcelFileName().equals( tableName )){
						try {
							return excel.getClass().newInstance() ;
						} catch ( Exception e) { 
							System.out.println( e.getMessage() );
						}
					}
				}
			}
		}
		
		return null ;
	}
	
	private static List<File> getHtableClass() throws Exception{
		
		List<File> htableFile = new ArrayList<File>() ;
		String classPath = ClassLoader.getSystemResource("").getPath() ; //class path
		String absCurrPath = packageMe.replace('.', File.separatorChar) ; //File.separatorChar
		File classes = new File(new File(classPath).getPath()+File.separatorChar+ absCurrPath ) ;
		
//		System.out.println(  classes.getPath()  +classes.exists()   );
//		System.out.println( new File(classPath).getAbsolutePath() );
		//Hello.listFile( new File(classPath));
		if(null == classes || !classes.isDirectory()){
			return htableFile ;
		}
		for(File filef: classes.listFiles() ){
			if( filef.isDirectory() ){
				continue ;
			}
			htableFile.add(filef) ;
		}
		
		//DEBUg
//		if( true ){
//			System.exit( 0 );
//		}
		
		return htableFile ;
	}
	
	private static List<String> getJava() throws Exception{
		List<String> classnames  = new ArrayList<String>() ;
		List<File> htableFile =getHtableClass() ;
		if( null !=htableFile && htableFile.size() > 0 ){
			for(File java:htableFile){
				String clzzName = java.getName() ;
				if(clzzName.contains(".class")){
					clzzName = clzzName.replace(".class", "") ;
				}
				classnames.add( HtableFactory.packageMe+"."+clzzName );
			}
		}
		return classnames ;
	}
	
	public static List<Object> getObject() throws Exception{
		List<String> javas = getJava() ;
		List<Object> objs  = new ArrayList<Object>() ;
		
		if( null != javas ){
			for(String clazz : javas ){
				 
				Object object = null ;;
				try {
					object = RefectUtil.invoke(clazz);
				} catch (Exception e) {
					System.out.println( e.getMessage() );
				}
				if( null == object ){
					continue ;
				}
				if( object instanceof ITable){
					objs.add( object ) ;
				}
			}
		}
		return objs ;
	}
	
	
	public static void main(String[] args) throws ClassNotFoundException {
		ITable htable = getItable("SITE_INFO_CDMA.xlsx") ;
		System.out.println( JSONObject.toJSONString( htable )  );
	}
	
}
