package com.qihoo.toolkit.generatepic.controller.impl;

import java.io.File;

import com.qihoo.toolkit.common.testlog.ToolKitLogger;
import com.qihoo.toolkit.common.util.ImageUtils;
import com.qihoo.toolkit.generatepic.controller.inter.Component;
import com.qihoo.toolkit.generatepic.controller.model.GenePicModel;

public class PngTypeDecorator extends Decorator {

	public PngTypeDecorator(Component component) {
		super(component);
		
	}

	@Override
	public void generatePic(GenePicModel genePicModel) throws Exception {
		super.generatePic(genePicModel);
		String genePicPath=genePicModel.getGenePicPath();
		String fileExtension=genePicModel.getFileExtension();
		String newFileName=genePicModel.getNewFileName();
		String newFileBaseName=genePicModel.getNewFileBaseName();

		if(genePicModel.isPng()==true && !"png".equals(fileExtension)){
			ImageUtils.converter(new File(genePicPath+"/"+newFileName),"png",new File(genePicPath+"/"+newFileBaseName+".png"));
			ToolKitLogger.info("图片类型转换，转换后图片：" + genePicPath+"/"+newFileBaseName+".png");
		}
		
	}
}
