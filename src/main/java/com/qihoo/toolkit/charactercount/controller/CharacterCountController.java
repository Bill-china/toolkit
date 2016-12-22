package com.qihoo.toolkit.charactercount.controller;

import java.util.HashMap;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import net.sf.json.JSONObject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.qihoo.toolkit.charactercount.service.CharacterCountService;
import com.qihoo.toolkit.common.testlog.ToolKitLogger;

@Controller
public class CharacterCountController {
	@Autowired
	private CharacterCountService characterCountService;
	
	@RequestMapping(value="/getCharCount.do")
	public @ResponseBody String getCharCount(@RequestParam("originalContent") String originalContent,@RequestParam("chineseMulti") int chineseMulti,HttpServletRequest request) {
		
		int chCharacter=0;
		int enCharacter=0;
		int spaceCharacter=0;
		int numCharacter=0;
		int otherCharacter=0;
		int totalCharacter=0;
		
		Map<String, Object> map=new HashMap<String, Object>();	
		if(null==originalContent || "".equals(originalContent)){
			map.put("result", "error");
		}else{
			for(int i=0;i<originalContent.length();i++){
				char tmp=originalContent.charAt(i);
				if( (tmp >= 'A' && tmp <= 'Z') || (tmp >= 'a' && tmp <= 'z')){
					enCharacter++;
				}else if(tmp >= '0' && tmp <= '9'){
					numCharacter++;
				}else if(tmp == ' '){
					spaceCharacter++;
				}else if(characterCountService.isChinese(tmp)){
					chCharacter++;
				}else{
					otherCharacter++;
				}
			}
			totalCharacter=chCharacter*chineseMulti + enCharacter + numCharacter + spaceCharacter + otherCharacter;
			map.put("result", "success");
			map.put("chinese", chCharacter);
			map.put("english", enCharacter);
			map.put("space", spaceCharacter);
			map.put("num", numCharacter);
			map.put("other", otherCharacter);
			map.put("total", totalCharacter);
		}
		
		String result=String.valueOf(JSONObject.fromObject(map));
		ToolKitLogger.info("统计字符，统计内容为："+originalContent);
		ToolKitLogger.info("统计字符，统计结果为："+result);
		return result;
	}
}
