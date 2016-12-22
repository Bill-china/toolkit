package com.qihoo.toolkit.common.testlog;

import org.apache.log4j.Logger;


public class ToolKitLogger {
	static Logger logger = Logger.getLogger(ToolKitLogger.class);
	
	public static void info(String info){
		logger.info(info);
	}
	public static void error(String info){
		logger.error(info);
	}
	public static void error(String info,Throwable throwable){
		logger.error(info,throwable);
	}
}
