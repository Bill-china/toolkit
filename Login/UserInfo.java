package com.qihoo.toolkit.Login;

public class UserInfo {
	private String cookieid;
	private String username;
	private String pwd;
	
	public UserInfo() {
		super();
		// TODO Auto-generated constructor stub
	}
	public UserInfo(String cookieid, String username, String pwd) {
		super();
		this.cookieid = cookieid;
		this.username = username;
		this.pwd = pwd;	
	}
	
	public String getUsername() {
		return username;
	}
	
	public void setUsername(String username) {
		this.username = username;
	}
	
	public String getPwd() {
		return pwd;
	}
	
	public void setPwd(String pwd) {
		this.pwd = pwd;
	}
	
	public String getCookieId() {
		return pwd;
	}
	
	public void setCookieId(String pwd) {
		this.pwd = pwd;
	}
}
