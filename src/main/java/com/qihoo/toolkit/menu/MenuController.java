package com.qihoo.toolkit.menu;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
public class MenuController {
	@RequestMapping("/index")
	public String indexManager(){
		return "index";
	}
	
	@RequestMapping("/generatePic")
	public String generatePicManager(){
		return "generatePic";
	}
	
	@RequestMapping("/characterCount")
	public String characterCount(){
		return "characterCount";
	}
	
	@RequestMapping("/redisCheck")
	public String redischeck(){
		return "redischeck";
	}
	
	@RequestMapping("/error_fileupload")
	public String error_fileupload(){
		return "error_fileupload";
	}
	
	@RequestMapping("/budgetUpdate")
	public String budgetUpdate(){
		return "budgetUpdate";
	}
	@RequestMapping("/balanceUpdate")
	public String balanceUpdate(){
		return "balanceUpdate";
	}
	@RequestMapping("/budgetExhausted")
	public String budgetExhausted(){
		return "budgetExhausted";
	}
	
	
}
