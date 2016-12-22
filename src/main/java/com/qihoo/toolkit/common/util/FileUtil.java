package com.qihoo.toolkit.common.util;

import java.io.File;

import com.qihoo.toolkit.common.testlog.ToolKitLogger;

public class FileUtil {
	
	public static void deleteDir(String dir){
		File file=new File(dir);
		File childrenFile[]=file.listFiles();
		try{
			for(int i=0;i<childrenFile.length;i++){
				if(childrenFile[i].isDirectory()){
					 deleteDir(dir+"/"+childrenFile[i].getName());
				}
				childrenFile[i].delete();
			}
		}catch(Exception e){
			ToolKitLogger.error("删除文件夹出错", e);
			e.printStackTrace();
		}
	}
}
