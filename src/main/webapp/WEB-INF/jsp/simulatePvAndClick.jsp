<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="zh-CN">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=utf-8" /> 



<link rel="stylesheet" href="js/codemirror-5.14.2/lib/codemirror.css">
<link rel="stylesheet" href="js/codemirror-5.14.2/addon/display/fullscreen.css">
<link rel="stylesheet" href="js/codemirror-5.14.2/addon/fold/foldgutter.css">
<link rel="stylesheet" href="js/codemirror-5.14.2/theme/dracula.css">
<link rel="stylesheet" href="js/codemirror-5.14.2/addon/dialog/dialog.css">
<link rel="stylesheet" href="js/codemirror-5.14.2/addon/search/matchesonscrollbar.css">

<link rel="stylesheet" type="text/css" href="js/jquery-ui-1.12.0/jquery-ui.css" /> 
<script type="text/javascript" src="js/jquery-ui-1.12.0/jquery-ui.js"></script>


<script type="text/javascript" src="js/codemirror-5.14.2/lib/codemirror.js"></script>
<!-- 折叠功能 -->
<script src="js/codemirror-5.14.2/addon/display/fullscreen.js"></script>
<script src="js/codemirror-5.14.2/addon/fold/foldcode.js"></script>
<script src="js/codemirror-5.14.2/addon/fold/foldgutter.js"></script>
<script src="js/codemirror-5.14.2/addon/fold/brace-fold.js"></script>
<script src="js/codemirror-5.14.2/addon/fold/comment-fold.js"></script>
<!-- 搜索功能 -->
<script src="js/codemirror-5.14.2/addon/dialog/dialog.js"></script>
<script src="js/codemirror-5.14.2/addon/search/searchcursor.js"></script>
<script src="js/codemirror-5.14.2/addon/search/search.js"></script>
<script src="js/codemirror-5.14.2/addon/search/matchesonscrollbar.js"></script>
<script src="js/codemirror-5.14.2/addon/scroll/annotatescrollbar.js"></script>
<script src="js/codemirror-5.14.2/addon/search/jump-to-line.js"></script>

<script src="js/codemirror-5.14.2/mode/javascript/javascript.js"></script>
<script type="text/javascript" src="js/redisCheck.js"></script>
</head> 
<body>

	<form class="form-inline" onsubmit="return monitorService();">
		   <div class="form-group">
		      <label for="checkValue">本机IP:</label>
		      <input type="text" class="form-control" id="checkValue" value="10.18.61.106" />
		 </div>
		 <input type="button" class="btn btn-primary" data-toggle="modal" data-target="#myModal" value="配置监控服务器信息" />
      	<input  class="btn btn-success" type="submit" id="monitorServiceButton" value="启动监控服务" />
	</form>
	 
     <div id="tabs">
     	<input type="hidden" id="pv_latesttime" value="0" >
     	<input type="hidden" id="click_latesttime" value="0" >
       <ul>
         <li><a href="#pv_redis_div">pv_redis</a></li>
         <li><a href="#pv_combineLog_div">pv_combineLog</a></li>
         <li><a href="#click_redis_div">click_redis</a></li>
         <li><a href="#click_cheatLog_div">click_cheatLog</a></li>
       </ul>
       <div id="pv_redis_div"><textarea id="pv_redis" name="pv_redis" rows="5"></textarea></div>
       <div id="pv_combineLog_div"><textarea id="pv_combineLog" name="pv_combineLog" rows="5"></textarea></div>
       <div id="click_redis_div"><textarea id="click_redis" name="click_redis" rows="5"></textarea></div>
       <div id="click_cheatLog_div"><textarea id="click_cheatLog" name="click_cheatLog" rows="5"></textarea></div>
     </div>



	<!-- Modal -->
	<div class="modal fade" id="myModal" tabindex="-1" role="dialog" aria-labelledby="myModalLabel">
	  <div class="modal-dialog" role="document">
	    <div class="modal-content">
	      <div class="modal-header">
	        <button type="button" class="close" data-dismiss="modal" aria-label="Close"><span aria-hidden="true">&times;</span></button>
	        <h4 class="modal-title" id="myModalLabel">监控服务配置</h4>
	      </div>
	      <div class="modal-body">
	        <form class="" >
		    	<label >测试环境:</label>
			 	<div class="radio">
					  <label>
					    <input type="radio" name="env" id="trunk" value="trunk" checked/>Trunk
					  </label>
				</div>
			 	<div class="radio">
					  <label>
					    <input type="radio" name="env" id="online" value="online"/>Online
					  </label>
				</div>
		    	<div class="form-group">
				    <label for="redisHostIp">redis监控服务器IP:</label>
				    <input type="text" class="form-control" id="redisHostIp" value="10.138.65.229" disabled />
			    </div>
			    <div class="form-group">
			       <label for="redisPort">redis监控端口号:</label>
			       <input type="text" class="form-control" id="redisPort" value="17801/17802" />
			     </div>
		
				 <div class="form-group">
				      <label for="fileHostIp">文件监控服务器IP:</label>
				      <input type="text" class="form-control" id="fileHostIp" value="10.138.65.227" disabled />
				 </div>
				 <div class="form-group">
				      <label for="filePort">文件监控端口号:</label>
				      <input type="text" class="form-control" id="filePort" value="7788" disabled />
				 </div>
				 <div class="form-group">
				      <label for="fileName">监控文件:</label>
				      <input type="text" class="form-control" id="fileName" value="combineLog/e_v2.cheatclick" disabled/>
				 </div>
		     </form>
	      </div>
	      <div class="modal-footer">
	        <button type="button" class="btn btn-default" data-dismiss="modal">确定</button>
	        <!-- <button type="button" class="btn btn-primary">Save changes</button> -->
	      </div>
	    </div>
	  </div>
	</div>
	
</body>
</html>
