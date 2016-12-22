package com.qihoo.toolkit.Login;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class UserInfoDaoImpl implements UserInfoDao{
	private Connection conn;
	private DbUserInfo Dbui;
	
	public UserInfo checkLogin(String name, String pwd) throws SQLException{
		UserInfo ui = null;
		Dbui = new DbUserInfo();
		conn = Dbui.DbConnect();
		String sql=String.format("select name, password from user_info where name='%s' and password=%s", name, pwd);
		 try {
			 	Statement stmt = conn.createStatement();
			 	ResultSet rs = stmt.executeQuery(sql);
	            if(rs.next()){
	            	ui = new UserInfo("", rs.getString("name"), rs.getString("password"));       
	            }
	        } catch (SQLException e) {
	            // TODO Auto-generated catch block
	            e.printStackTrace();
	        }finally{
	        	Dbui.Dbisconnect();	        
	        	}
	        return ui;
	    }
}
