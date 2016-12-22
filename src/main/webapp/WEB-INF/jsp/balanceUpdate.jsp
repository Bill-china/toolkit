<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="zh-CN">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" /> 

<script type="text/javascript" src="js/balanceUpdate.js"></script>
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
	<p>用户余额</p>
	<form class="form-inline" onsubmit="">
		<div>
			<label class=""  for="">选择方式：</label>
				<input type="radio" name="radiobalance" class="radioItem" value="modequery" data-toggle="">查询  	
				<input type="radio" name="radiobalance" class="radioItem" value="modeupdate" data-toggle="" checked="checked">更新		
		</div>
		<label class="" style="margin-top:10px">选择环境：</label>
		<select name="chineseMulti" style="margin-top:10px" class="form-control" style="width:8%" data-style="btn-primary" id="selectpdtline" onchange="SelectChange();">
								<option value="" selected="selected">trunk</option>
								<option value="" >online</option>
		</select>
	</form>

	<form class="form-inline" onsubmit="" style="margin-top:10px">
		<div class="form-group">
			<label for="checkValue">用户id:</label>
		    <input type="text" class="form-control" style="margin-left:38px" id="uid" value="" placeholder="请输入用户id"/>
		</div>
	</form>
	
	<form class="form-inline" onsubmit="" style="margin-top:10px">
		<div class="form-group" id="divbalance">
			<label for="checkValue">新余额:</label>
		    <input type="text" class="form-control" style="margin-left:10px" id="newbalance" value="" placeholder="请输入金额"/>
		</div>
	</form>
	
	<input type="button" id="btnbalance" value="确定" class="btn btn-default" style="margin-left:10px;margin-top:10px" data-dismiss="modal" onclick="setUserBalance()"/>
	<div class="container" style="margin-top:10px" id="balancemsg"></div>
</div>

</body>
</html>
