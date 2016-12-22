package com.qihoo.toolkit.generatepic.service;

import java.util.Date;

import org.quartz.Job;
import org.quartz.JobExecutionContext;
import org.quartz.JobExecutionException;

import com.qihoo.toolkit.common.testlog.ToolKitLogger;
import com.qihoo.toolkit.common.util.FileUtil;

public class CleanFileJob{

	public void execute() throws JobExecutionException{

		String url=Thread.currentThread().getContextClassLoader().getResource("").getPath();
		url=url.replaceFirst("target/classes/", "src/main/webapp/upload");
		ToolKitLogger.info("清理上传文件夹工作开始,目录为:"+url);
		FileUtil.deleteDir(url);
		ToolKitLogger.info("清理上传文件夹工作结束");
	}
	

}
