package com.qihoo.toolkit.Login;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class LoginController {
	@RequestMapping("/Login")
	public String generatePicManager(){
		return "Login";
	}
}