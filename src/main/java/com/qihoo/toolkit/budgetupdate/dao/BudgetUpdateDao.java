package com.qihoo.toolkit.budgetupdate.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.qihoo.toolkit.db.DBUtil;
import com.jcraft.jsch.Session;
import com.qihoo.toolkit.budgetupdate.model.BudgetUpdateModel;
import com.qihoo.toolkit.db.HttpUtil;
import redis.clients.jedis.Jedis;
import net.sf.json.JSONObject;

public class BudgetUpdateDao {
	
	// 查询产品线预算
    public String queryProductLineBudget(String uid, String match, String env) throws Exception {
    	String rhost = "106.120.162.231";
    	int rport = 3302;
		if (env.endsWith("online")) {
			rport = 3301;
		}	
		DBUtil dbconn = new DBUtil();
		Session session = dbconn.sshConnect(rhost, rport);
        Connection conn = dbconn.DbConnect();
        int dbId = 0;
        dbId = Integer.parseInt(uid) % 10;
        String quotaProducetLine = null;
        if (match.equals("mv")) 
        	quotaProducetLine = "mv_quota";
        else if (match.equals("buer"))
        	quotaProducetLine = "app_quota";
        else if (match.equals("sou"))
        	quotaProducetLine = "sou_quota";
        else
        	quotaProducetLine = "ruyi_quota";
        String sql = "select " + quotaProducetLine + " from ad_user_quota_" + Integer.toString(dbId) + " where ad_user_id = " + uid + ";";     
        System.out.println(sql);
        Statement stmt;
        try {
        	stmt = conn.createStatement();        
            ResultSet rs = stmt.executeQuery(sql);
            if (rs.next()) {
            	BudgetUpdateModel model = new BudgetUpdateModel();
            	model.setProductLineBudget(rs.getString(1));
            	return rs.getString(1);
            }         
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
        	conn.close();
        	session.disconnect();
        }
        return null;
    }
    
    public int updateProductLineBudget(String uid, String productLine, String env, String budget) throws Exception {
        String key = null;
        String mvBudget = "";
        String djBudget = "";
        int budget_type = 1;
        String rhost = "10.138.65.229";
        int rport = 16371;
        String url = "http://trunk.eapi.e.360.cn/uc/user/updateBudget";
        StringBuilder sb = new StringBuilder();
        sb.append("budget_");
        sb.append(uid);
        if (productLine.equals("mv")) {
        	sb.append("_mediav");
        	budget_type = 4;
        }
        else if (productLine.equals("buer")) {
        	sb.append("_app");
        	budget_type = 2;
        }
        	
        else if (productLine.equals("sou")) {
        	sb.append("_dianjing");
        	budget_type = 1;
        }
        else {
        	sb.append("_ruyi");
        	budget_type = 3;
        }
        key = sb.toString();
        
        if(env.equals("online")) {
        	rport = 6371;
        	url = "http://online.eapi.e.360.cn/uc/user/updateBudget";
        }     	
       	
        Jedis redis = new Jedis (rhost, rport);
        redis.del(key);
        redis.disconnect();
        
        sb = new StringBuilder();
        if (budget_type == 4)
        	mvBudget = budget;
        else 
        	djBudget = budget;
        sb.append("budget=");
        sb.append(djBudget);
        sb.append("&mediav_budget=");
        sb.append(mvBudget);
        sb.append("&budget_type=");
        sb.append(budget_type);
        sb.append("&userId=");
        sb.append(uid);
        System.out.println(sb.toString());
        String str = HttpUtil.sendPost(url, sb.toString());
        System.out.println(str);
        JSONObject js = JSONObject.fromObject(str);
        return js.getInt("errno");
    }
	
	// 更新产品线预算
    public void updatePlanBudget(String uid, String pid, String productLine, String env, String budget) throws Exception {
    	String key = null;
        String rhost = "10.138.65.229";
        int rport = 16371;
        StringBuilder sb = new StringBuilder();
    	String url = "http://trunk.eapi.e.360.cn/dianjing/campaign/changeBudget";
    	if(env.equals("online")) {
    		url = "http://online.eapi.e.360.cn/dianjing/campaign/changeBudget";
    	}
        if (productLine.equals("buer")) {
        	url = "http://trunk.eapi.e.360.cn/app/plan/changeBudget";
        	if(env.equals("online"))
        		url = "http://online.eapi.e.360.cn/app/plan/changeBudget";
        }
        
    }
       
}
