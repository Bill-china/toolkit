package com.qihoo.toolkit.common.redisclient;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.qihoo.toolkit.common.testlog.ToolKitLogger;

public class RedisClientListener {
	
	private RedisClientListener(){
		
	}
	private static RedisClientListener instance=null;
	private ConcurrentHashMap<String,String> datas=new ConcurrentHashMap<String,String>();
	//这里List<RedisClient>需要监听两个端口，一个pv，一个click
	private ConcurrentHashMap<String,List<RedisClient>> clients=new ConcurrentHashMap<String,List<RedisClient>>();
	
	public static RedisClientListener getInstance(){
		//先判断实例是否存在，不存在再进行加锁处理  
		if(instance==null){
			synchronized(RedisClientListener.class){//同一时刻只允许一个线程进入加锁的部分  
				if(instance==null){//第二重判断
					instance=new RedisClientListener();
				}
			}
		}
		return instance;
	}
	
	public String getMonitorData(String session){
		return datas.get(session);
	}
	
	public void setMonitorData(String session,String data){
		datas.put(session, data);
	}
	
	public void clearMonitorData(String session){
		datas.remove(session);
	}
	
	public void setRedisClient(String session,List<RedisClient> redisClients){
		clients.put(session, redisClients);
	}
	
	public boolean stopRedisClient(String session){
		try{
			List<RedisClient> redisClients=clients.get(session);
			for(RedisClient rc:redisClients){
				rc.stop();
			}
			clients.remove(session);
			this.clearMonitorData(session);
			ToolKitLogger.info("redis监控服务已正常停止");
		}catch(Exception e){
			return false;
		}
		return true;
	}

}
