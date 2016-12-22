package com.qihoo.toolkit.common.telnetclient;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;

import com.qihoo.toolkit.common.testlog.ToolKitLogger;

public class TelnetClientManager {
	
	private static TelnetClientManager instance=null;
	private ConcurrentHashMap<String,NetTelnetClient> clients=new ConcurrentHashMap<String,NetTelnetClient>();
	public TelnetClientManager(){
		
	}
	public static TelnetClientManager getInstance(){
		if(instance==null){//先判断实例是否存在，不存在再进行加锁处理  
			synchronized(TelnetClientManager.class){//同一时刻只允许一个线程进入加锁的部分  
				if(instance==null){//第二重判断
					instance=new TelnetClientManager();
				}
			}
		}
		return instance;
	}
	
	public NetTelnetClient getTelnetClient(String key){
		if(clients.get(key)!=null){
			return clients.get(key);
		}
		return null;
	}
	
	public synchronized void addTelnetClient(String key,NetTelnetClient client){
		if(clients.get(key)==null){
			clients.put(key, client);
		}
	}
	
	public synchronized void removeTelnetClient(String key){
		if(clients.get(key)!=null){
			clients.remove(key);
		}
	}

	public String stopTelnetClient(String session){
		for(Entry<String,NetTelnetClient> entry : clients.entrySet()){
			String key = entry.getKey();
			NetTelnetClient client = entry.getValue();
			if(key.startsWith(session)){
				removeTelnetClient(key);
				client.close();
			}
		}
		return "success";
	}
	
	public void createTelnetClient(String key){
		if(clients.get(key)==null){
			try{
				String[] param=key.split("_");
				NetTelnetClient client=new NetTelnetClient(param[1],Integer.parseInt(param[2]));
				addTelnetClient(key,client);
			}catch(Exception e){
				ToolKitLogger.error("Socket创建失败"+key, e);
			}
		}

	}
}
