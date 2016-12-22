package com.qihoo.toolkit.generatepic.controller.impl;

import org.apache.commons.io.FilenameUtils;

import com.qihoo.toolkit.common.testlog.ToolKitLogger;
import com.qihoo.toolkit.generatepic.controller.inter.Component;
import com.qihoo.toolkit.generatepic.controller.model.GenePicModel;

public class ConcreteComponent implements Component {

	public void generatePic(GenePicModel genePicModel) throws Exception {
		
		String fileExtension=FilenameUtils.getExtension(genePicModel.getFileName()).toLowerCase();
		String fileBaseName=FilenameUtils.getBaseName(genePicModel.getFileName());
		genePicModel.setFileExtension(fileExtension);
		genePicModel.setFileBaseName(fileBaseName);
		ToolKitLogger.info("图片信息初始化");

	}

}
