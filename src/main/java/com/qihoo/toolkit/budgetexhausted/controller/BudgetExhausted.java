package com.qihoo.toolkit.budgetexhausted.controller;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.qihoo.toolkit.balanceupdate.dao.BalanceUpdateDao;

import net.sf.json.JSONObject;
import com.qihoo.toolkit.db.DBUtil;
import java.net.URL;

//import org.apache.xmlrpc.XmlRpcException;
//import org.apache.xmlrpc.client.XmlRpcClient;
//import org.apache.xmlrpc.client.XmlRpcClientConfigImpl;
//import org.apache.xmlrpc.client.XmlRpcCommonsTransportFactory;
//import org.apache.xmlrpc.client.XmlRpcLiteHttpTransportFactory;
//import org.apache.xmlrpc.client.XmlRpcLocalTransportFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.net.MalformedURLException;

@Controller
public class BudgetExhausted {
		@RequestMapping(value="/budgetExhausted.do")
		public @ResponseBody String ProductLineBudgetExhausted(@RequestParam("env") String env, @RequestParam("pdtline") String pdtline, @RequestParam("uid") String uid, HttpServletRequest request) throws Exception {
//			System.out.println( productLine);
//			XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
//			config.setServerURL(new URL("http://10.138.65.216:8899/RPC2"));
//			XmlRpcClient client = new XmlRpcClient();
//			client.setConfig(config);
			
			String dbLoc = env.equals("trunk") ? "trunk_child" : "online_child";
			String cloCurQuotaPdtline = null;
			String cloPdtlineCost = null;
			String cloPdtlineYesCost = null;
			String queryField = null;
			if(pdtline.equals("sou")) {
				cloCurQuotaPdtline = "sou_quota";
				cloPdtlineCost = "sou_cost";
				cloPdtlineYesCost = "yesterday_sou_cost";
				queryField = "sou_quota, sou_cost, yesterday_sou_cost";
			}
			else if(pdtline.equals("buer")) {
				cloCurQuotaPdtline = "app_quota";
				cloPdtlineCost = "app_cost";
				cloPdtlineYesCost = "yesterday_app_cost";
				queryField = "app_quota, app_cost, yesterday_app_cost";
			}
			else if(pdtline.equals("ruyi")) {
				cloCurQuotaPdtline = "ruyi_quota";
				cloPdtlineCost = "ruyi_cost";
				cloPdtlineYesCost = "yesterday_ruyi_cost";
				queryField = "ruyi_quota, ruyi_cost, yesterday_ruyi_cost";
			}
			else {
				cloCurQuotaPdtline = "mv_quota";
				cloPdtlineCost = "mv_cost";
				cloPdtlineYesCost = "yesterday_mv_cost";
				queryField = "mv_quota, mv_cost, yesterday_mv_cost";
			}
			int dbId = 0;
			dbId = Integer.parseInt(uid) % 10;
			String tableName = "ad_user_quota";
			tableName += Integer.toString(dbId);
			String dbName = "ad_quota";
			Object[] paramsInit = new Object[]{dbLoc, dbName};
			Object[] patamsQuery = new Object[]{tableName, queryField, uid};
			
			Map<String, Object> map=new HashMap<String, Object>();	

			String result = null;
			return result;
		}
		
		public @ResponseBody String PlanBudgetExhausted(@RequestParam("env") String env, @RequestParam("pdtline") String pdtline, @RequestParam("uid") String uid, HttpServletRequest request) throws Exception {
			return null;
		}
		
/*		public static void main(String[] args) {
			try {
				XmlRpcClientConfigImpl config = new XmlRpcClientConfigImpl();
				config.setEncoding("UTF-8");
				config.setServerURL(new URL("http://10.138.65.216:7111/RPC2"));
				XmlRpcClient client = new XmlRpcClient();
				client.setConfig(config);
				
				String dbLoc = "trunk_child";
				String queryField = "sou_quota, sou_cost, yesterday_sou_cost";
				String tableName = "ad_user_quota_7";
				String dbName = "ad_quota";
				Object[] paramsInit = new Object[]{dbLoc, dbName};
				Object[] patamsQuery = new Object[]{dbLoc, dbName, tableName, queryField, 160185657};
				//查询产品线当日预算、花费、昨日花费
				List<String> fl = new ArrayList<String>();
				String res = (String) client.execute("query", patamsQuery);
				float pdtQuota = Float.parseFloat(res.split(",")[0]);
				float pdtCost = Float.parseFloat(res.split(",")[1]);
				float pdtYesCost = Float.parseFloat(res.split(",")[2]);
				//计算撞线时，消费金额
				float clickPrice = pdtQuota - pdtCost - pdtYesCost;
				
				//返回的结果是字符串类型，所以强制转换res为String类型，然后打印出来即可
				System.out.println(pdtQuota);
				System.out.println(pdtCost);
				System.out.println(pdtYesCost);
			} catch (MalformedURLException e) {
	            e.printStackTrace();
	            } catch (XmlRpcException e) {
	            e.printStackTrace();
	        }
	    }*/
		
}
