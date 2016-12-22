
<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="zh-CN">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" /> 

<script type="text/javascript" src="js/budgetExhausted.js"></script>
<style type="text/css">
.d1 {border-style:none;}
.d2 {border-style:solid;}
.d3 {border-style:dotted;}
.d4 {border-style:dashed;}
.d5 {border-style:double;}
.d6 {border-style:groove;}
.d7 {border-style:ridge;}
.d8 {border-style:inset;}
.d9 {border-style:outset;}
.center {
  width: auto;
  display: table;
  margin-left: auto;
  margin-right: auto;
}

</style>
</head> 

<body>

<div class="center">
	<p>预算撞线</p>
	<form class="form-inline" onsubmit="">
		<div>
			<label class=""  for="">撞线类型：</label>
			<input type="radio" name="radioquota" class="radioItem" value="modepdt" data-toggle="" checked="checked">产品线	
			<input type="radio" name="radioquota" class="radioItem" value="modeplan" data-toggle="">计划  					
		</div>
		
		<div>
			<label class=""  for="">产品线类型：</label>
			<input type="radio" name="radiobgttype" class="radioItem" value="pdtsou" data-toggle="" checked="checked">搜索	
			<input type="radio" name="radiobgttype" class="radioItem" value="pdtbuer" data-toggle="">布尔
			<input type="radio" name="radiobgttype" class="radioItem" value="pdtruyi" data-toggle="">如意	
			<input type="radio" name="radiobgttype" class="radioItem" value="pdtmv" data-toggle="">mv			
		</div>
		<label class="" style="margin-top:10px">选择环境：</label>
		<select name="chineseMulti" style="margin-top:10px" class="form-control" style="width:8%" data-style="btn-primary" id="selectenv" onchange="SelectChange();">
								<option value="" selected="selected">trunk</option>
								<option value="" >online</option>
		</select>
		
		<div style="margin-top:10px">
			<label for="checkValue">用户id:</label>
		    <input type="text" class="form-control" style="margin-left:17px" id="uid" value="" placeholder="请输入用户id"/>
		</div>
		
		<div class="form-group" id="divplan" style="margin-top:10px;display: none">
			<label for="checkValue" id="labplan">计划id:</label>
			<input type="text" id="valueplan" class="form-control" style="margin-left:17px" id="planbudget" placeholder="请输入计划id"/>
			<label for="checkValue" id="labgrp" style="margin-left:38px" >组id:</label>
			<input type="text" id="valuegrp" class="form-control" style="margin-left:17px" id="planbudget" placeholder="请输入组id"/>
			<label for="checkValue" id="labad" style="margin-left:38px" >创意id:</label>
			<input type="text" id="valuead" class="form-control" style="margin-left:17px" id="planbudget" placeholder="请输入创意id"/>
		</div>
		
	</form>
	
	<input type="button" id="btnbgt" value="确定" class="btn btn-default" style="margin-left:10px;margin-top:10px" data-dismiss="modal" onclick="setUserBalance()"/>
	<div class="container" style="margin-top:10px" id="bgtmsg"></div>
	
	<table class="table" id="quotadata">
	<thead id="t"> 

	</thead>
		<tbody>
		</tbody>
	</table>

	
</div>

</body>
</html>
