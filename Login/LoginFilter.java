package com.qihoo.toolkit.Login;

import java.io.IOException;
import java.sql.SQLException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

public class LoginFilter implements Filter {
	private UserInfoDaoImpl UserInfoDaoImpl;
	private int expires = 20;// 20s
	@Override
    public void destroy() {
        // TODO Auto-generated method stub
         
    }
	
	@Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
		// 第一步造型
        HttpServletRequest req = (HttpServletRequest) request;
        HttpServletResponse resp = (HttpServletResponse) response;
        String uri = req.getRequestURI();
        String url = req.getRequestURL().toString();
        // 登陆请求、初始请求直接放行
        if ("/toolkit/LoginServlet".equals(uri) || "/toolkit/".equals(uri)) {
        	//放行        
        	chain.doFilter(request, response);        
        	return;      
        	}
        // 不是登陆请求的话，判断是否有cookie
        
        Cookie cookies[] = req.getCookies();
        if (cookies != null && cookies.length > 0) {
        	String userName = null;  
            String password = null;
            String userinfo = null;
            // 判断cookie中的用户名和密码是否和数据库中的一致，如果一致则放行，否则转发请求到登陆页面
            //如果cookie时间过期，这里就获取不到cookie
            for (Cookie cookie : cookies) {
            	if ("userinfo".equals(cookie.getName())) {
            		userinfo = cookie.getValue();
            		String[] arrUserInfo = userinfo.split(":");
            		userName = arrUserInfo[0];
            		password = arrUserInfo[1];
            		UserInfoDaoImpl = new UserInfoDaoImpl();
                    try {
                        UserInfo user = UserInfoDaoImpl.checkLogin(userName, password);
                        //这里不仅要判断用户名和密码无误，且cookie不过期，则更新cookie失效时间往下跳
                        if (null != user) {
                        	cookie.setMaxAge(expires);
                        	resp.addCookie(cookie);
                        	chain.doFilter(req, resp);
                        	return;
                        }
                        else {
                        	// 重定向到登陆界面 
                        	req.getRequestDispatcher("Login.do").forward(req, resp);
//                        	resp.sendRedirect("/toolkit/Login.do");
                        	return;
                        }
                    }
                    catch (SQLException e){
                    	e.printStackTrace();
                    }
            	}
            }
            req.getRequestDispatcher("Login.do").forward(req, resp);
//            resp.sendRedirect("/toolkit/Login.do");
        	return;
        }
        else {
        	req.getRequestDispatcher("Login.do").forward(req, resp);
//        	resp.sendRedirect("/toolkit/Login.do");
            return;
//        	chain.doFilter(request, response);
//            return;
        }

	}
	
	  @Override
	    public void init(FilterConfig arg0) throws ServletException {
	        // TODO Auto-generated method stub
	         
	    }
	
}
	
