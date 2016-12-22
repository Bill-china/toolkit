<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="zh-CN">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" /> 
<script type="text/javascript" src="js/generatePic.js"></script>
<script type="text/javascript" src="js/ajaxfileupload.js"></script>

</head>


<body>
	<input type="file" id="ImportPicInput" name="myfile" style="display:none" />
	<div class="input-append">
		<label for="importPicName">上传原始图片：</label>
		<input type="text" class="input-large" id="importPicName" />
		<a class="btn btn-default" onclick="$('#ImportPicInput').click();" >打开</a>
	</div>
	
	<table id="PicParamTable" class="table table-striped table-bordered table-hover">
			<thead>
				<tr>
					<th colspan="2">生成图片的宽*高</th>
					<th colspan="3">生成图片的格式</th>
				</tr>
			</thead>
			<tr class="PicParamInfo" >
				<td width="30%">
					<div class="input-group">
					<span class="input-group-addon">宽</span>
					<input  type="text" class="form-control width" name="width" value="" />
					</div>
				</td>
				<td  width="30%">
					<div class="input-group">
					<span class="input-group-addon">高</span>
					<input  type="text" class="form-control height" name="height" value="" />
					</div>
				</td>
				<td  width="20%">
					<label class="checkbox-inline"><input type="checkbox" checked="checked" class="jpg" value=""> jpg</label>
				    <label class="checkbox-inline"><input type="checkbox" checked="checked" class="png" value=""> png</label>
				    <label class="checkbox-inline"><input type="checkbox" checked="checked" class="gif" value=""> gif</label>
				    <label class="checkbox-inline"><input type="checkbox" checked="checked" class="bmp" value=""> bmp</label>
				</td>
				<td  width="10%"><input class="btn btn-default" type="button" value="+" onclick="addOneToPicParam(this)" /></td>
				<td  width="10%"><input class="btn btn-default" type="button" value="-" onclick="deleteOneToPicParam(this)" /></td>
			</tr>
	</table>


	<table class="table">
			<tr>
				<td width="10%">
					<input  class="btn btn-success" type="button" id="generatePics" name="generatePics" value="生成图片" onclick="generatePics();"/>
				</td>
				<td width="30%">
					<div id="picSucResult" style="display:none">
						<strong>图片生成成功，请点击</strong><a id="downLoadLink" href="">这里</a>
					</div>
					<div id="picFailResult" style="display:none">
						<strong>图片生成失败</strong><p id="errorMsg"></p>
					</div>
				</td>
				<td width="60%"></td>
			</tr>
	</table>
	
</body>
</html>
