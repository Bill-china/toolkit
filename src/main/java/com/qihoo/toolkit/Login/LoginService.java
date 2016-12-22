package com.qihoo.toolkit.Login;

public class LoginService {
	public static int i;
	
	static {
		i = init();
	}
//	public static int i = 20;
	
	public static int init() {
		return 10;
	}
	
	public static void main(String args[]) {
		System.out.println(i);
	}
}
