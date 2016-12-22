package com.qihoo.toolkit.common.util;

import java.io.File;
import java.text.ParseException;
import java.text.ParsePosition;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;


public class DateUtil {
	public static Date stringToDate(String dateString,String dateFormat){
		
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		Date date = new Date();
		try {
			 date = sdf.parse(dateString);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return date;
		
	}
	
	public static String dateToString(Date date,String dateFormat){
		
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		String dateString = new String();
		dateString = sdf.format(date);
		
		return dateString;
		
	}
	
	public static Date dateFormat(Date date,String dateFormat){
		
		SimpleDateFormat formatter = new SimpleDateFormat(dateFormat);
		String dateString = formatter.format(date);
		ParsePosition pos = new ParsePosition(0);
		Date currentTimeTemp = formatter.parse(dateString, pos);
		return currentTimeTemp;

	}

	public static String getNowTime(){
		String dateFormat = "yyyy-MM-dd HH:mm:ss";
		Date now = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		return sdf.format(now);
	}
	
	public static String getNowTimeData(){
		String dateFormat = "yyyyMMddHHmmss";
		Date now = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		return sdf.format(now);
	}
	
	public static void main(String[] args) {
		
		
	}
}
