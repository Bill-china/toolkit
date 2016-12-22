package com.qihoo.toolkit.generatepic.controller;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import com.qihoo.toolkit.common.util.CompressedFileUtil;
import com.qihoo.toolkit.common.util.ImageUtils;
import com.qihoo.toolkit.common.util.DateUtil;
import com.qihoo.toolkit.generatepic.controller.impl.BmpTypeDecorator;
import com.qihoo.toolkit.generatepic.controller.impl.ConcreteComponent;
import com.qihoo.toolkit.generatepic.controller.impl.CutImageDecorator;
import com.qihoo.toolkit.generatepic.controller.impl.GifTypeDecorator;
import com.qihoo.toolkit.generatepic.controller.impl.JpgTypeDecorator;
import com.qihoo.toolkit.generatepic.controller.impl.PngTypeDecorator;
import com.qihoo.toolkit.generatepic.controller.inter.Component;
import com.qihoo.toolkit.generatepic.controller.model.GenePicModel;


@Controller
public class GenePicController {


	@RequestMapping(value="/importPicFile.do",produces="text/html;charset=utf-8")
	public @ResponseBody String importPicFile(@RequestParam("picParams") String picParams,@RequestParam MultipartFile myfile,HttpServletRequest request){

		
		Map<String,Object> map=new HashMap<String,Object>();
		if(myfile.isEmpty()){
			map.put("result", "error");
			map.put("msg", "上传文件不能为空");
		}else{
			String originalFilename=myfile.getOriginalFilename();
			String fileBaseName=FilenameUtils.getBaseName(originalFilename);
			String floderName=fileBaseName+"_"+DateUtil.getNowTimeData();
			try{
				
				String genePicPath=request.getSession().getServletContext().getRealPath("/upload/"+floderName);
				String uploadPath=request.getSession().getServletContext().getRealPath("/upload");
				FileUtils.copyInputStreamToFile(myfile.getInputStream(), new File(genePicPath,originalFilename));
				
				Component component=new ConcreteComponent();
				Component cutImageDecorator=new CutImageDecorator(component);
				Component jpgTypeDecorator=new JpgTypeDecorator(cutImageDecorator);
				Component pngTypeDecorator=new PngTypeDecorator(jpgTypeDecorator);
				Component gifTypeDecorator=new GifTypeDecorator(pngTypeDecorator);
				Component bmpTypeDecorator=new BmpTypeDecorator(gifTypeDecorator);
				
				JSONArray itemArray = JSONArray.fromObject(picParams);
				for (Object object : itemArray) {
					JSONObject itemObject = (JSONObject) object;
					GenePicModel gpModel=new GenePicModel();
					gpModel.setWidth(Integer.parseInt(itemObject.getString("width")));
					gpModel.setHeight(Integer.parseInt(itemObject.getString("height")));
					gpModel.setJpg(Boolean.parseBoolean(itemObject.getString("jpg")));
					gpModel.setPng(Boolean.parseBoolean(itemObject.getString("png")));
					gpModel.setGif(Boolean.parseBoolean(itemObject.getString("gif")));
					gpModel.setBmp(Boolean.parseBoolean(itemObject.getString("bmp")));
					gpModel.setFileName(originalFilename);
					gpModel.setGenePicPath(genePicPath);

					bmpTypeDecorator.generatePic(gpModel);
				}
				//图片压缩
				CompressedFileUtil.compressedFile(genePicPath, uploadPath);
				map.put("result", "success");
				map.put("filename", floderName+".zip");
				
			}catch (Exception e) {
				map.put("result", "error");
				map.put("msg",e.getMessage());
				
			}
		}
		String result=String.valueOf(JSONObject.fromObject(map));
		return result;
	}
	
	
}
