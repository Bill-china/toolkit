package com.qihoo.toolkit.balanceupdate.controller;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.qihoo.toolkit.balanceupdate.dao.BalanceUpdateDao;

import net.sf.json.JSONObject;

@Controller
public class BalanceUpdateController {
	@RequestMapping(value="/getUserBalance.do")
	public @ResponseBody String getProductLineBudget(@RequestParam("uid") String userid, HttpServletRequest request) throws Exception {
//		System.out.println( productLine);
		Map<String, Object> map=new HashMap<String, Object>();	
		BalanceUpdateDao dao=new BalanceUpdateDao();
		String str = dao.queryUserBalance(userid);
		map.put("budget", str);
		String result=String.valueOf(JSONObject.fromObject(map));
		return result;
	}
}
