package com.qihoo.toolkit.Login;

import java.sql.SQLException;

public interface UserInfoDao {
	public UserInfo checkLogin(String name, String pwd) throws SQLException; 
}
