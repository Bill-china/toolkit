package com.qihoo.toolkit.generatepic.controller.model;

public class GenePicModel {
	String fileName;
	String fileBaseName;
	String fileExtension;
	
	String genePicPath;//生成图片带时间戳的文件夹路径
	String newFileName;//剪裁后的图片的名称，带尺寸
	String newFileBaseName;
	
	int width;
	int height;
	boolean jpg;
	boolean png;
	boolean gif;
	boolean bmp;
	
	
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) {
		this.fileName = fileName;
	}
	public String getFileBaseName() {
		return fileBaseName;
	}
	public void setFileBaseName(String fileBaseName) {
		this.fileBaseName = fileBaseName;
	}
	public String getFileExtension() {
		return fileExtension;
	}
	public void setFileExtension(String fileExtension) {
		this.fileExtension = fileExtension;
	}
	public String getGenePicPath() {
		return genePicPath;
	}
	public void setGenePicPath(String genePicPath) {
		this.genePicPath = genePicPath;
	}
	public String getNewFileName() {
		return newFileName;
	}
	public void setNewFileName(String newFileName) {
		this.newFileName = newFileName;
	}
	public String getNewFileBaseName() {
		return newFileBaseName;
	}
	public void setNewFileBaseName(String newFileBaseName) {
		this.newFileBaseName = newFileBaseName;
	}
	public int getWidth() {
		return width;
	}
	public void setWidth(int width) {
		this.width = width;
	}
	public int getHeight() {
		return height;
	}
	public void setHeight(int height) {
		this.height = height;
	}
	public boolean isJpg() {
		return jpg;
	}
	public void setJpg(boolean jpg) {
		this.jpg = jpg;
	}
	public boolean isPng() {
		return png;
	}
	public void setPng(boolean png) {
		this.png = png;
	}
	public boolean isGif() {
		return gif;
	}
	public void setGif(boolean gif) {
		this.gif = gif;
	}
	public boolean isBmp() {
		return bmp;
	}
	public void setBmp(boolean bmp) {
		this.bmp = bmp;
	}
	
	public String buildNewFileName(){
		this.newFileName = fileBaseName+"_"+width+"_"+height+"."+fileExtension;
		return this.newFileName;
	}
	
	public String buildNewFileBaseName(){
		this.newFileBaseName = fileBaseName+"_"+width+"_"+height;
		return this.newFileBaseName;
	}
	


}
