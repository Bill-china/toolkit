package com.qihoo.toolkit.db;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;

import org.springframework.core.env.SystemEnvironmentPropertySource;

public class DBUtil {
	
	//调试用
	public static int lport = 8888;//本地端口（随便取）  
	public static String remoteHost = "106.120.162.231";//远程MySQL服务器  
	public static int remotePort = 3301;//远程MySQL服务端口
	
//	private static final String URL="jdbc:mysql://10.138.65.216:3306/ad_quota?user=test_dj&password=test_dj@360&useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=false&maxReconnects=10";
	private static final String URL="jdbc:mysql://127.0.0.1:8888/ad_quota?user=test_dj&password=test_dj@360&useUnicode=true&characterEncoding=UTF-8";
    private static Connection conn = null;
    private static Session session = null;
    
//	public static void sshGo(String rhost, int rport) {
    public static Session sshConnect(String rhost, int rport) {
		if(rhost == null | rport == 0) {
			System.out.println("连接数据库参数有误");
			return null;	
		}
		
	    String user = "wangxubing";//SSH连接用户名
	    String password = "dabing-Pass-123";//SSH连接密码
	    String host = "10.138.65.216";//SSH服务器
	    int port = 22;//SSH访问端口
	    try {
	      JSch jsch = new JSch();
	      Session session = jsch.getSession(user, host, port);
	      session.setPassword(password);
	      session.setConfig("StrictHostKeyChecking", "no");
	      session.connect();
	      System.out.println(session.getServerVersion());//这里打印SSH服务器版本信息
	      int assinged_port = session.setPortForwardingL(lport, rhost, rport);
	      System.out.println("localhost:" + assinged_port + " -> " + rhost + ":" + rport);
	      return session;
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	    return null;
	}
    
    public static void sshDisconnect() throws Exception {
    	 if (session != null)
			 session.disconnect();
    }
	
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
	
	public static void Dbisconnect() throws ClassNotFoundException, SQLException{
		 if (conn != null)
			 conn.close();
	}
    
	//调试用
    public static void main(String[] args) throws Exception{    
    	//ssh远程登录 
    	session = sshConnect(remoteHost, remotePort);  	 
    	 try {
             //加载驱动程序
             Class.forName("com.mysql.jdbc.Driver");
             //获取数据库连接
//             conn= DriverManager.getConnection(URL, USER, PASSWORD);
             conn= DriverManager.getConnection(URL);
             System.out.println(conn);
        	 try {
        		//创建statement对象
        		 Statement stmt = conn.createStatement();
        		 System.out.println(stmt);
     	         ResultSet rs=stmt.executeQuery("select * from ad_user_quota_7 where ad_user_id=160185657");
     	         while(rs.next()){
     	            System.out.println(rs.getInt(1)+","+rs.getInt(2)+","+rs.getInt(3)+","+rs.getDate("cur_date"));
     	        } 
        	 }catch (SQLException e) {
        		 System.out.println("SQLException1: " + e.getMessage());
        		 e.printStackTrace();
        	    } 
             
         } catch (ClassNotFoundException e) {
             // TODO Auto-generated catch block
         	System.out.println("SQLException2: " + e.getMessage());
             e.printStackTrace();
         } catch (SQLException e) {
             // TODO Auto-generated catch block
         	System.out.println("SQLException: " + e.getMessage());
             e.printStackTrace();
         }
    	 finally {
    		 Dbisconnect();
    		 sshDisconnect();
    	 }
    }
}
