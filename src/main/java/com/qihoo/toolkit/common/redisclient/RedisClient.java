package com.qihoo.toolkit.common.redisclient;


import com.qihoo.toolkit.common.testlog.ToolKitLogger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisMonitor;

public class RedisClient {
	private Jedis  jedis;
	private Thread monitorThread;
	private String checkValue="";
	private String session;
	
	public RedisClient(String hostIp,int port,String checkValue,String session){
		jedis=new Jedis(hostIp,port);
		this.checkValue=checkValue;
		this.session=session;
		
	}
	
	public void start(){
		monitorThread=new Thread(new Monitor());
		monitorThread.start();
	}
	
	public boolean status(){
		
		return jedis.isConnected();
	}
	
	public class Monitor implements Runnable{

		public void run() {
			try{
				jedis.monitor(getJedisMonitor());
			}catch(Exception e){
				ToolKitLogger.error("Redis监控服务异常停止", e);
			}
		}
	}
	
	public JedisMonitor getJedisMonitor(){
		JedisMonitor jm = new JedisMonitor(){

			@Override
			public void onCommand(String command) {
				try{
					if(command.contains(" ")){
						String[] content = command.split(" ");
						if(content.length >= 5){
							if(content[3].replace("\"", "").equals("RPUSH")){
								if(command.contains(checkValue)){//筛选包含本机ip（checkValue）的redis日志
									RedisClientListener.getInstance().setMonitorData(session, command);
								}
							}
						}
					}
				}catch(Exception e){
					ToolKitLogger.error("Redis监控服务监控失败", e);
				}
			}
		};
		return jm;
	}
	
	public boolean stop(){
		try{
			monitorThread.interrupt();
			jedis.disconnect();
		}catch(Exception e){
			ToolKitLogger.error("停止Redis监控服务异常", e);
		}
		return !status();
	}
	
}
