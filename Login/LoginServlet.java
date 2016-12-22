package com.qihoo.toolkit.Login;

import java.util.List;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.ArrayList;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

/**
 * Servlet implementation class LoginServlet
 */
public class LoginServlet extends HttpServlet {
	private UserInfoDaoImpl UserInfoDaoImpl;
	private int expires = 20;// 20s
	
	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		response.setCharacterEncoding("UTF-8");
		response.setContentType("text/html;charset=UTF-8");
		PrintWriter out = response.getWriter();
		String username = request.getParameter("username");
		String pwd = request.getParameter("pwd");
		String uri = request.getRequestURI();
		String[] arrUri = uri.split("/");
		uri = arrUri[arrUri.length - 1].equals("LoginServlet") ? "index.do" : arrUri[arrUri.length - 1];
		String url = request.getRequestURL().toString();
		UserInfoDaoImpl = new UserInfoDaoImpl();
		//校验用户名和密码是否正确
		try {
			UserInfo ui = UserInfoDaoImpl.checkLogin(username, pwd);
			if (null != ui) {
				HttpSession session = request.getSession();
				session.setAttribute("username", username);       
				session.setAttribute("password", pwd);
				Cookie cookie = new Cookie("JSESSIONID", session.getId());
				// 设置cookie的存储时长        
				cookie.setMaxAge(expires);        
				// 把cookie发送给浏览器        
				response.addCookie(cookie);        
//				request.getRequestDispatcher(uri).forward(request, response);
				response.sendRedirect(uri);
			}
			else {
				//用户名或密码有误
	            out.write("用户名或密码不正确！");
				request.getRequestDispatcher("Login.do").forward(request, response);
//	            response.sendRedirect("/toolkit/Login.do");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		doGet(request, response);
	}

}


