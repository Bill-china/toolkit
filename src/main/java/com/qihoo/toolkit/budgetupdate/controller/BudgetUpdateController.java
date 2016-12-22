package com.qihoo.toolkit.budgetupdate.controller;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import com.qihoo.toolkit.budgetupdate.dao.BudgetUpdateDao;

import net.sf.json.JSONObject;

@Controller
public class BudgetUpdateController {
	
	@RequestMapping(value="/getProductLineBudget.do")
	public @ResponseBody String getProductLineBudget(@RequestParam("uid") String userid, @RequestParam("pdtline") String productLine, @RequestParam("env") String env, HttpServletRequest request) throws Exception {
		System.out.println( productLine);
		Map<String, Object> map=new HashMap<String, Object>();	
		BudgetUpdateDao dao=new BudgetUpdateDao();
		String str = dao.queryProductLineBudget(userid, productLine, env);
		System.out.println( str);
		if (str != null) {
			map.put("budget", str);
			map.put("status", 200);
		}
		String result=String.valueOf(JSONObject.fromObject(map));
		return result;
	}
	
	@RequestMapping(value="/setProductLineBudget.do")
	public @ResponseBody String setProductLineBudget(@RequestParam("uid") String userid, @RequestParam("pdtline") String productLine, @RequestParam("env") String env, @RequestParam("budget") String budget, HttpServletRequest request) throws Exception {
		BudgetUpdateDao dao=new BudgetUpdateDao();
		int errno = dao.updateProductLineBudget(userid, productLine, env, budget);
		Map<String, Object> map=new HashMap<String, Object>();
		map.put("errno", errno);
		String result=String.valueOf(JSONObject.fromObject(map));
		return result;
	}
	
//	public @ResponseBody String getPlanBudget(@RequestParam("pid") String pid, HttpServletRequest request) {
//		BudgetUpdateDao dao=new BudgetUpdateDao();
//		return dao.getPlanBudget();
//	}
//	
//	public @ResponseBody void setPlanBudget(@RequestParam("planbudget") String planBudget, HttpServletRequest request) {
//		BudgetUpdateDao dao=new BudgetUpdateDao();
//		dao.updatePlanBudget(uid, pid, productLine, env, planBudget);	
//	}
}
