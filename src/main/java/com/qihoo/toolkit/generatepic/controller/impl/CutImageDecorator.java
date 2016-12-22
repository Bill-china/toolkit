package com.qihoo.toolkit.generatepic.controller.impl;

import java.awt.image.BufferedImage;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

import javax.imageio.ImageIO;

import org.apache.commons.io.FilenameUtils;

import com.qihoo.toolkit.common.testlog.ToolKitLogger;
import com.qihoo.toolkit.common.util.ImageUtils;
import com.qihoo.toolkit.generatepic.controller.inter.Component;
import com.qihoo.toolkit.generatepic.controller.model.GenePicModel;

public class CutImageDecorator extends Decorator {

	public CutImageDecorator(Component component) {
		
		super(component);
	}
	
	@Override
	public void generatePic(GenePicModel genePicModel) throws Exception {
		super.generatePic(genePicModel);
		String genePicPath=genePicModel.getGenePicPath();
		String fileName=genePicModel.getFileName();
		String fileExtension=genePicModel.getFileExtension();
		int width= genePicModel.getWidth();
		int height= genePicModel.getHeight();
		//新的文件名在旧的文件名基础上增加了尺寸
		String newFileName =genePicModel.buildNewFileName();
		String newFileBaseName =genePicModel.buildNewFileBaseName();
		
		BufferedImage BImage = ImageIO.read(new FileInputStream(genePicPath+"/"+fileName));
		int originalWidth=BImage.getWidth();
		int originalHeight=BImage.getHeight();
		if(width>originalWidth || height>originalHeight){
			throw new Exception("("+width+"*"+height+") is larger than the original size ("+originalWidth+"*"+originalHeight+")");
		}else{
			//截图
			ImageUtils.cutImage(new FileInputStream(genePicPath+"/"+fileName), 
					new FileOutputStream(genePicPath+"/"+newFileName),fileExtension,0,0,width,height);
			ToolKitLogger.info("图片截取，生成图片：" + genePicPath+"/"+newFileName);
		}
		
	}

}
