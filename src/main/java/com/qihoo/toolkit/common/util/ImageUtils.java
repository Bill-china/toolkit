package com.qihoo.toolkit.common.util;

import java.awt.Color;
import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.io.*;
import java.util.Iterator;

import javax.imageio.ImageIO;
import javax.imageio.ImageReadParam;
import javax.imageio.ImageReader;
import javax.imageio.stream.ImageInputStream;

public class ImageUtils {
	
	public static void cutJPG(InputStream input,OutputStream out,int x,int y,int width,int height) throws IOException{
		ImageInputStream imageStream =null;
		try{
			Iterator<ImageReader> readers=ImageIO.getImageReadersByFormatName("jpg");
			ImageReader reader=readers.next();
			imageStream= ImageIO.createImageInputStream(input);
			reader.setInput(imageStream,true);
			ImageReadParam param=reader.getDefaultReadParam();
			Rectangle rect=new Rectangle(x,y,width,height);
			param.setSourceRegion(rect);
			BufferedImage bi=reader.read(0,param);
			ImageIO.write(bi, "jpg", out);
		}finally{
			imageStream.close();
			input.close();
			out.close();
		}
	}
	
	public static void cutPNG(InputStream input,OutputStream out,int x,int y,int width,int height) throws IOException{
		ImageInputStream imageStream =null;
		try{
			Iterator<ImageReader> readers=ImageIO.getImageReadersByFormatName("png");
			ImageReader reader=readers.next();
			imageStream= ImageIO.createImageInputStream(input);
			reader.setInput(imageStream,true);
			ImageReadParam param=reader.getDefaultReadParam();
			Rectangle rect=new Rectangle(x,y,width,height);
			param.setSourceRegion(rect);
			BufferedImage bi=reader.read(0,param);
			ImageIO.write(bi, "png", out);
		}finally{
			imageStream.close();
			input.close();
			out.close();
		}
	}
	
	public static void cutImage(InputStream input, OutputStream out, String type,int x,  
            int y, int width, int height) throws IOException {  
        ImageInputStream imageStream = null;  
        try {  
            String imageType=(null==type||"".equals(type))?"jpg":type;  
            Iterator<ImageReader> readers = ImageIO.getImageReadersByFormatName(imageType);  
            ImageReader reader = readers.next();  
            imageStream = ImageIO.createImageInputStream(input);  
            reader.setInput(imageStream, true);  
            ImageReadParam param = reader.getDefaultReadParam();  
            Rectangle rect = new Rectangle(x, y, width, height);  
            param.setSourceRegion(rect);  
            BufferedImage bi = reader.read(0, param);  
            ImageIO.write(bi, imageType, out);  
            
        } finally {
            imageStream.close();
            input.close();
			out.close();
        }  
    }

	
	
	public static void converter(File imgfile,String format,File formatFile) throws IOException{
		imgfile.canRead();
		BufferedImage bi = ImageIO.read(imgfile);  
		// create a blank, RGB, same width and height, and a white background
		BufferedImage newBufferedImage = new BufferedImage(bi.getWidth(), bi.getHeight(), BufferedImage.TYPE_INT_RGB);
		newBufferedImage.createGraphics().drawImage(bi, 0, 0, Color.WHITE, null);
		ImageIO.write(newBufferedImage, format, formatFile);
       
	}
	
	public static void main(String[] args) throws Exception {
        //ImageUtils.cutJPG(new FileInputStream("E:\\图片1.jpg"), new FileOutputStream("E:\\图片1-1.jpg"), 0,0,160,120);  
        ImageUtils.converter(new File("E:\\图片1.png"),"jpg",new File("E:\\图片2.jpg"));
        //ImageUtils.converter(new File("E:\\图片1-1.jpg"),"gif",new File("E:\\图片1-1.gif"));
        //ImageUtils.cutPNG(new FileInputStream("c:\\1.png"), new FileOutputStream("c:\\test3.png"), 0,0,50,40);  
    }
	
  
}
