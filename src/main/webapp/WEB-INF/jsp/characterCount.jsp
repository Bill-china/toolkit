<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="zh-CN">
<head>
<script type="text/javascript" src="js/characterCount.js"></script>
</head>


<body>
	<textarea  id="originalContent" class="form-control" rows="5"  ></textarea>
	</br>
	<input  class="btn btn-success" type="button" id="charCount" value="统计字数" onclick="getCharCount();"/>
	</br></br>
	<table class="table">
		<tbody>
			<tr>
				<td >
					<div class="input-group">
						<span class="input-group-addon">中文</span>
						<input type="input" class="form-control" id="chinese" value="" disabled>
					</div>
				</td>
				<td >*</td>
				<td >
					<select name="chineseMulti" id="chineseMulti" onchange="changeChMulti();">
												<option value="1" selected="selected">1</option>
												<option value="2" >2</option>
					</select>
				</td>
				<td >
					<div class="input-group">
						<span class="input-group-addon">英文</span>
						<input type="input" class="form-control" id="english" value="" disabled>
					</div>
				</td>
				<td >
					<div class="input-group">
						<span class="input-group-addon">数字</span>
						<input type="input" class="form-control" id="num" value="" disabled>
					</div>
				</td>
				<td >
					<div class="input-group">
						<span class="input-group-addon">空格</span>
						<input type="input"  class="form-control" id="space" value="" disabled>
					</div>
				</td>
				<td >
					<div class="input-group">
						<span class="input-group-addon">其他</span>
						<input type="input" class="form-control" id="other" value="" disabled>
					</div>
				</td>
			</tr>
			<tr>
			<td>
				<div class="input-group">
					<span class="input-group-addon">总计</span>
					<input type="input"  class="form-control" id="total" value="" disabled>
				</div>
			</td>
			</tr>
		</tbody>
	</table>
	
</body>
</html>
