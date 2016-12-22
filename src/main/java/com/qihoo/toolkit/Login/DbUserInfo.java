package com.qihoo.toolkit.Login;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;


public class DbUserInfo {
	private static final String URL="jdbc:mysql://localhost:3306/test?user=bing&password=bing&useUnicode=true&characterEncoding=UTF-8";
    private static Connection conn = null;
    
//	public static List<UserInfo> list = new ArrayList<UserInfo>();
//	static{
//		list.add(new UserInfo("1jfjfjf", "aaa", "123"));
//		list.add(new UserInfo("2jfjfjf", "bbb", "123"));
//		list.add(new UserInfo("3jfjfjf", "ccc", "123"));
//	}
//	public static List getAll(){
//		return list;
//	}

    public Connection DbConnect(){

	   	 try {
	         //加载驱动程序
	         Class.forName("com.mysql.jdbc.Driver");
	         //获取数据库连接
	//         conn= DriverManager.getConnection(URL, USER, PASSWORD);
	         conn= DriverManager.getConnection(URL);
	         System.out.println(conn);
	         return conn;
	     } catch (ClassNotFoundException e) {
	         // TODO Auto-generated catch block
	     	System.out.println("SQLException: " + e.getMessage());
	         e.printStackTrace();
	     } catch (SQLException e) {
	         // TODO Auto-generated catch block
	     	System.out.println("SQLException: " + e.getMessage());
	     	System.out.println("数据库连接失败！");
	         e.printStackTrace();
	     }
	   	 return conn;
	}
	
	public static void Dbisconnect() throws SQLException {
		 if (conn != null)
			 conn.close();
	}
	
	public static void main(String[] args) throws Exception{
		try {
            //加载驱动程序
            Class.forName("com.mysql.jdbc.Driver");
            //获取数据库连接
//            conn= DriverManager.getConnection(URL, USER, PASSWORD);
            conn= DriverManager.getConnection(URL);
            System.out.println(conn);
	       	try {
	       		//创建statement对象
	       		 Statement stmt = conn.createStatement();
	       		 System.out.println(stmt);
	    	         ResultSet rs=stmt.executeQuery("select * from user_info where ID = 1");
	    	         while(rs.next()){
	    	            System.out.println(rs.getString("cookie_id")+","+rs.getString("name")+","+rs.getString("password"));
//	    	        	System.out.println(rs);
	    	        } 
	       	}catch (SQLException e) {
	       		 System.out.println("SQLException1: " + e.getMessage());
	       		 e.printStackTrace();
	       	} 
            
        } catch (ClassNotFoundException | SQLException e) {
            // TODO Auto-generated catch block
        	System.out.println("SQLException2: " + e.getMessage());
            e.printStackTrace();
        } 
	   	 finally {
	   		Dbisconnect();
	   	 }
		System.out.println("hello word!");
	}
}
