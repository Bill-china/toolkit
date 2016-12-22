package com.qihoo.toolkit.common.telnetclient;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.net.SocketException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.exec.OS;
import org.apache.commons.net.telnet.TelnetClient;

import com.qihoo.toolkit.common.testlog.ToolKitLogger;



public class NetTelnetClient {
	
	private TelnetClient telnet=null;
	private InputStreamReader in;
	private PrintStream out;
	private BufferedReader br;
	private String respCharset="UTF-8";
	
	public NetTelnetClient(String host,int port){
		if(telnet==null){
			telnet=new TelnetClient();
			try{
				telnet.connect(host, port);
				in=new InputStreamReader(telnet.getInputStream(),respCharset);//
				out= new PrintStream(telnet.getOutputStream());//
				
			}catch(SocketException e){
				ToolKitLogger.error("连接发生异常", e);
			}catch(IOException e){
				ToolKitLogger.error("读写数据异常", e);
			}
		}
	}
	
	public void close(){
		
		if(telnet!=null){
			try{
				telnet.disconnect();
			}catch(IOException e){
				ToolKitLogger.error("关闭连接异常", e);
			}
		}
	}

	
	public void write(String value){
		try{
			out.println(value);
			out.flush();
		}catch(Exception e){
			ToolKitLogger.error("读写数据异常", e);
		}
	}
	
	public String readUntil(String pattern){
		br= new BufferedReader(in);
		String str="";
		try{
			while(!str.endsWith(pattern) && telnet.isConnected()){
				str+=br.readLine();
			}
		}catch(Exception e){
			ToolKitLogger.error("读写数据异常", e);
		}
		return str;
		
	}
	
	public String sendCommand(String command,String pattern){
		try{
			this.write(command);
			return Executors.newCachedThreadPool().submit(new DataThread(pattern)).get(1, TimeUnit.SECONDS);
		}catch(Exception e){
			ToolKitLogger.error("发送命令异常", e);
			return "发送命令异常"+e.getMessage();
		}
	}
	
	
	
	private class DataThread implements Callable<String>{
		private String pattern;
		public DataThread(String pattern){
			this.pattern=pattern;
		}
		public String call() throws Exception{
			return readUntil(pattern);
		}
	}

	public static void main(String[] args) {
		
		NetTelnetClient n = new NetTelnetClient("10.138.65.227",7788);
		String rs = n.sendCommand("!scanfile combineLog c99af9b1599f2ccc 1", "success!");
		System.out.println(rs);
		n.close();
	}

}
