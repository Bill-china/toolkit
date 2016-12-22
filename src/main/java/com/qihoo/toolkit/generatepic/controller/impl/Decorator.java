package com.qihoo.toolkit.generatepic.controller.impl;

import org.apache.commons.io.FilenameUtils;

import com.qihoo.toolkit.generatepic.controller.inter.Component;
import com.qihoo.toolkit.generatepic.controller.model.GenePicModel;

public class Decorator implements Component {

	private Component component;

	
	public Decorator(Component component){
		
		this.component = component;
		
	}
	public void generatePic(GenePicModel genePicModel) throws Exception{
		
		component.generatePic(genePicModel);
		
	}

}
