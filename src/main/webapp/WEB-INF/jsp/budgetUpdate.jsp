<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="zh-CN">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" /> 

<script type="text/javascript" src="js/budgetUpdate.js"></script>
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
.box1 {width:40%; float:left; display:inline;} 
.box2 {width:40%; float:left; display:inline;}
.search  
{  
    margin: 10px 0;  
    border-style:dashed;  
    height :50%;  
    width:45%;  
    float:left;  
} 
.search2  
{  
    margin: 10px 0;  
    border-style:dashed;  
    height :50%;  
    width:45%;  
    float:right;  
} 
</style>
</head> 

<body>

<div style='width:50%;float:left;height:700px;'>
	<p>产品线预算</p>
	<form class="form-inline" onsubmit="">
		<div>
			<label class=""  for="">选择方式：</label>
				<input type="radio" name="radiopdtline" class="radioItem" value="modequery" data-toggle="" checked="checked">查询  	
				<input type="radio" name="radiopdtline" class="radioItem" value="modeupdate" data-toggle="">更新		
		</div>
		
		<div>
			<label class="" style="margin-top:10px">选择环境：</label>
			<select name="chineseMulti" style="margin-top:10px" class="form-control" style="width:8%" data-style="btn-primary" id="selectenv" onchange="SelectChange();">
									<option value="" selected="selected">trunk</option>
									<option value="" >online</option>
			</select>
		</div>
		
		<label class="" style="margin-top:10px">选择产品线：</label>
		<select name="chineseMulti" style="margin-top:10px" class="form-control" style="width:8%" data-style="btn-primary" id="selectpdtline" onchange="SelectChange();">
								<option value="" selected="selected">搜索</option>
								<option value="" >布尔</option>
								<option value="" >mv</option>
								<option value="" >如意</option>
		</select>

	</form>

	<form class="form-inline" onsubmit="" style="margin-top:10px">
		<div class="form-group">
			<label for="checkValue">用户id:</label>
		    <input type="text" class="form-control" style="margin-left:38px" id="uid" value="" placeholder="请输入用户id"/>
		</div>
	</form>

	<form class="form-inline" onsubmit="" style="margin-top:10px">
		<div class="form-group" id="divProductline" style="display: none;">
			<label for="checkValue">产品线预算:</label>
		    <input type="text" class="form-control" style="margin-left:10px" id="productlinebgt" value="" placeholder="请输入预算金额"/>
		</div>
	</form>
	<input type="button" id="btnpdtline" value="确定" class="btn btn-default" style="margin-left:10px;margin-top:10px" data-dismiss="modal" onclick="setProductLineBudget()"/>
	<div class="container" style="margin-top:10px" id="bgtmsg"></div>
</div>

<div style='width:1px;border:1px solid red;float:left;height:240px;'><!--这个div模拟一条红色的垂直分割线--></div>

<div style='width:49%;float:left;height:700px;'>
	<p>计划预算</p>
	<form class="form-inline" onsubmit="">
		<div>
			<label class=""  for="">选择方式：</label>
				<input type="radio" class="radioItem1" name="radioplan" value="modequery" data-toggle="" checked="checked">查询  	
				<input type="radio" class="radioItem1" name="radioplan" value="modeupdate" data-toggle="">更新
		</div>
		
		<div>
			<label class="" style="margin-top:10px">选择环境：</label>
			<select name="chineseMulti" style="margin-top:10px" class="form-control" style="width:8%" data-style="btn-primary" id="selectenv" onchange="SelectChange();">
									<option value="" selected="selected">trunk</option>
									<option value="" >online</option>
			</select>
		</div>
		
		<label class="" style="margin-top:10px">选择产品线：</label>
		<select name="chineseMulti" style="margin-top:10px" class="form-control" style="width:8%" data-style="btn-primary" id="selectpdtline" onchange="SelectChange();">
								<option value="" selected="selected">搜索</option>
								<option value="" >布尔</option>
								<option value="" >如意</option>
		</select>
		
		<div style="margin-top:10px">
			<label for="checkValue" >计划id:</label>
			<input type="text" class="form-control"  style="margin-left:38px" id="pid" placeholder="请输入计划id"/>
		</div>
		<div style="margin-top:10px">
			<label for="checkValue" >用户id:</label>
		    <input type="text" class="form-control" style="margin-left:38px" id="uid" value="" placeholder="请输入用户id"/>
		</div>
	</form>
	<form class="form-inline" style="margin-top:10px" onsubmit="">
		<div class="form-group" id="divplan" style="display: none;">
			<label for="checkValue">计划预算:</label>
			<input type="text" class="form-control" style="margin-left:22px" id="planbudget" placeholder="请输入预算金额"/>
		</div>
	</form>
	<input type="button" id="btnplanquota" value="确定" class="btn btn-default" style="margin-left:10px;margin-top:10px" data-dismiss="modal" onclick="setPlanBudget()"/>	
</div>

</body>
</html>
