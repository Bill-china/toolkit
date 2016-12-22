package com.qihoo.toolkit.common.util;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.tools.zip.ZipEntry;
import org.apache.tools.zip.ZipOutputStream;

public class CompressedFileUtil {
	
	public static void compressedFile(String resourcesPath,String targetPath)throws Exception{
		File resourcesFile=new File(resourcesPath);
		File targetFile=new File(targetPath);
		
		if(!targetFile.exists()){
			targetFile.mkdirs();
		}
		
		String targetName=resourcesFile.getName()+".zip";
		FileOutputStream outputStream=new FileOutputStream(targetPath+"/"+targetName);
		ZipOutputStream out=new ZipOutputStream(new BufferedOutputStream(outputStream));
		out.setEncoding("UTF-8");
		createCompressedFile(out,resourcesFile,"");
		out.close();
	}
	
	public static void createCompressedFile(ZipOutputStream out,File file,String parentPath) throws IOException{
		
		if(file.isDirectory()){
			parentPath+=file.getName()+"/";
			File[] files=file.listFiles();
			for(int i=0;i<files.length;i++){
				createCompressedFile(out,files[i],parentPath);
			}
		}else{
			FileInputStream fis=null;
			try{
				fis=new FileInputStream(file);
				ZipEntry zipEntry=new ZipEntry(parentPath+file.getName());
				zipEntry.setUnixMode(644);
				out.putNextEntry(zipEntry);
				int len=0;
				byte[] buffer=new byte[1024];
				while((len=fis.read(buffer))>0){
					out.write(buffer,0,len);
					out.flush();
				}
				
			}catch(Exception e){
				e.printStackTrace();
			}finally{
				try{
					if(fis!=null){
						fis.close();
					}
				}catch(IOException e){
					e.printStackTrace();
				}
			}
		}
	}
	
	public static void main(String[] args){
		
		/*try{
			CompressedFileUtil.compressedFile("E:\\图片1", "E:\\");
			System.out.println("压缩文件已生成");
		}catch(Exception e){
			System.out.println("压缩文件生成失败");
			e.printStackTrace();
		}*/
	}
	

}
