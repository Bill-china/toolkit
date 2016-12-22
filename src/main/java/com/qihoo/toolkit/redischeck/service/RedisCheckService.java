package com.qihoo.toolkit.redischeck.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.qihoo.toolkit.common.redisclient.RedisClient;
import com.qihoo.toolkit.common.redisclient.RedisClientListener;
import com.qihoo.toolkit.common.telnetclient.NetTelnetClient;
import com.qihoo.toolkit.common.telnetclient.TelnetClientManager;
import com.qihoo.toolkit.common.testlog.ToolKitLogger;


@Service
public class RedisCheckService {
	
	public void startRedisMonitor(String session,String redisPort,String redisHostIp,String checkValue){
		
		List<RedisClient> redisList = new ArrayList<RedisClient>();
		if(redisPort.contains("/")){
			String[] ports= redisPort.split("/");
			for(String p:ports){
				RedisClient rc=new RedisClient(redisHostIp,Integer.parseInt(p),checkValue,session);
				rc.start();
				redisList.add(rc);
			}
		}else{
			RedisClient rc=new RedisClient(redisHostIp,Integer.parseInt(redisPort),checkValue,session);
			rc.start();
			redisList.add(rc);
		}
		
		RedisClientListener.getInstance().setRedisClient(session, redisList);
	}
	
	public void startFileMonitor(String session,String fileHostIp,String filePort){
		
		String telnetClientKey=session+"_"+fileHostIp+"_"+filePort;
		TelnetClientManager.getInstance().createTelnetClient(telnetClientKey);
	}
	
	public HashMap<String,String> getRedisLog(String session,String pv_latesttime,String click_latesttime,HashMap<String,String> map){
		String redisLog = RedisClientListener.getInstance().getMonitorData(session);
		if(redisLog!=null){
			String[] content = redisLog.split(" ");
			String curtime = content[0];
			for(int i=0;i<5;i++){
				redisLog = redisLog.replace(content[i], "");
			}
			redisLog = redisLog.trim().replace("\\", "");
			redisLog = redisLog.substring(1, redisLog.length()-1);
			
			if(redisLog.contains("\"type\":\"view\"") ){
				map.put("type", "view");
				if(!curtime.equals(pv_latesttime)){
					map.put("redisLog", redisLog);
					map.put("latesttime", curtime);
				}else{
					map.put("redisLog", "noMoreNew");
				}
			}else if(redisLog.contains("\"type\":\"click\"")){
				map.put("type", "click");
				if(!curtime.equals(click_latesttime)){
					map.put("redisLog", redisLog);
					map.put("latesttime", curtime);
				}else{
					map.put("redisLog", "noMoreNew");
				}
			}
		}else{
			map.put("type", "notype");
		}
		return map;
	}
	
	
	public String getViewId(String redisLog){
		String view_id=null;
		try{
			JSONObject infoJson = JSONObject.fromObject(redisLog);
			JSONArray jsonArray=infoJson.getJSONArray("data");
			view_id=jsonArray.getJSONObject(0).getString("view_id");
			
		}catch(Exception e){
			ToolKitLogger.error("获取view_id失败", e);
		}
		return view_id;
	}
	
	
	public String getClickId(String redisLog){
		String click_id=null;
		try{
			JSONObject infoJson = JSONObject.fromObject(redisLog);
			click_id=infoJson.getString("click_id");
			
		}catch(Exception e){
			ToolKitLogger.error("获取click_id失败", e);
		}
		return click_id;
	}
	
	public String getFileMonitorResult(String session,String fileHostIp,String filePort,String filename,String checkValue){
		
		String key=session+"_"+fileHostIp+"_"+filePort;
		NetTelnetClient ntClient=TelnetClientManager.getInstance().getTelnetClient(key);
		String rs =ntClient.sendCommand("!scanfile "+filename+" "+checkValue+" 1", "success!");
		if(!rs.startsWith("exception")){
			rs = StringUtils.substringBetween(rs, "INFO:","success!").trim();
		}else if(rs.contains("TimeoutException")){
			ToolKitLogger.info("远程文件监控服务已关闭!");
			TelnetClientManager.getInstance().stopTelnetClient(session);
		}else if(rs.contains("NullPointerException")){
			ToolKitLogger.info("socket连接创建失败，请确认远程文件监控服务已启动!");
		}
		return rs;
	}
	
	
}
