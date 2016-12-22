package com.qihoo.toolkit.generatepic.controller.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;

import com.qihoo.toolkit.common.testlog.ToolKitLogger;
import com.qihoo.toolkit.common.util.ImageUtils;
import com.qihoo.toolkit.generatepic.controller.inter.Component;
import com.qihoo.toolkit.generatepic.controller.model.GenePicModel;

public class JpgTypeDecorator extends Decorator {

	public JpgTypeDecorator(Component component) {
		super(component);
	}
	
	@Override
	public void generatePic(GenePicModel genePicModel) throws Exception {
		super.generatePic(genePicModel);
		String genePicPath=genePicModel.getGenePicPath();
		String fileExtension=genePicModel.getFileExtension();
		String newFileName=genePicModel.getNewFileName();
		String newFileBaseName=genePicModel.getNewFileBaseName();

		if(genePicModel.isJpg()==true && !"jpg".equals(fileExtension)){
			ImageUtils.converter(new File(genePicPath+"/"+newFileName),"jpg",new File(genePicPath+"/"+newFileBaseName+".jpg"));
			ToolKitLogger.info("图片类型转换，转换后图片：" + genePicPath+"/"+newFileBaseName+".jpg");
		}
		
	}

}
