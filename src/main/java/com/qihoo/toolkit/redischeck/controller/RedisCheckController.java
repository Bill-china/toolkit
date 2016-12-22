package com.qihoo.toolkit.redischeck.controller;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import net.sf.json.JSONObject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.qihoo.toolkit.charactercount.service.CharacterCountService;
import com.qihoo.toolkit.common.redisclient.RedisClient;
import com.qihoo.toolkit.common.redisclient.RedisClientListener;
import com.qihoo.toolkit.common.telnetclient.NetTelnetClient;
import com.qihoo.toolkit.common.telnetclient.TelnetClientManager;
import com.qihoo.toolkit.common.testlog.ToolKitLogger;
import com.qihoo.toolkit.redischeck.service.RedisCheckService;

@Controller
public class RedisCheckController {
	
	@Autowired
	private RedisCheckService redisCheckService;
	
	@RequestMapping(value="/startMonitorService")
	public @ResponseBody void startMonitorService(@RequestParam("fileHostIp") String fileHostIp,@RequestParam("filePort") String filePort,
			@RequestParam("redisHostIp") String redisHostIp,@RequestParam("redisPort") String redisPort,
			@RequestParam("checkValue") String checkValue,HttpServletRequest request) throws InterruptedException{
		
		String session=request.getSession().getId();
		redisCheckService.startRedisMonitor(session,redisPort,redisHostIp,checkValue);
		redisCheckService.startFileMonitor(session, fileHostIp, filePort);
		
	}
	
	@RequestMapping(value="/stopMonitorService")
	public @ResponseBody void stopMonitorService(HttpServletRequest request) throws InterruptedException{

		String session=request.getSession().getId();
		RedisClientListener.getInstance().stopRedisClient(session);
		TelnetClientManager.getInstance().stopTelnetClient(session);
	}
	
	@RequestMapping(value="/getMonitorContent",produces="text/html;charset=utf-8")
	public @ResponseBody String getMonitorContent(@RequestParam("fileHostIp") String fileHostIp,@RequestParam("filePort") String filePort,
			@RequestParam("pv_latesttime") String pv_latesttime,
			@RequestParam("click_latesttime") String click_latesttime,HttpServletRequest request) {
		HashMap<String,String> map=new HashMap<String,String>();
		String session = request.getSession().getId();
		
		map=redisCheckService.getRedisLog(session,pv_latesttime,click_latesttime,map);
		if(map.get("type")=="view" && map.get("redisLog")!="noMoreNew"){
			String redisLog=map.get("redisLog");
			String view_id=redisCheckService.getViewId(redisLog);
			String combineLog=redisCheckService.getFileMonitorResult(session,fileHostIp,filePort,"combineLog",view_id);
			map.put("combineLog", combineLog);
		}else if(map.get("type")=="click" && map.get("redisLog")!="noMoreNew"){
			String redisLog=map.get("redisLog");
			String click_id=redisCheckService.getClickId(redisLog);
			String cheatClick=redisCheckService.getFileMonitorResult(session,fileHostIp,filePort,"e_v2.cheatclick",click_id);
			map.put("cheatClick", cheatClick);
		}

		return String.valueOf(JSONObject.fromObject(map));
	}
}
