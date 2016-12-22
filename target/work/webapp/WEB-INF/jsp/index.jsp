<%@ page language="java" contentType="text/html; charset=utf-8"
	pageEncoding="utf-8"%>
<%@taglib prefix="c" uri="http://java.sun.com/jsp/jstl/core"%>
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">
<html xmlns="http://www.w3.org/1999/xhtml" lang="en">
<head>
<title>测试工具平台</title>


<link rel="stylesheet" href="js/bootstrap-3.3.5/dist/css/bootstrap.css">
<link rel="stylesheet" href="js/bootstrap-3.3.5/dist/css/bootstrap-theme.min.css">
<link rel="stylesheet" href="css/navbar-fixed-top.css">

<script type="text/javascript" src="js/jquery-1.10.2.min.js"></script> 
<script src="js/bootstrap-3.3.5/dist/js/bootstrap.min.js"></script>
<script type="text/javascript" src="js/index.js"></script>
</head>


<body>
<div>
 	<nav class="navbar navbar-inverse navbar-fixed-top" >
      <div class="container">
        <div class="navbar-header">
          <button type="button" class="navbar-toggle collapsed" data-toggle="collapse" data-target="#navbar" aria-expanded="false" aria-controls="navbar">
            <span class="sr-only">Toggle navigation</span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
            <span class="icon-bar"></span>
          </button>
          <a class="navbar-brand" href="#">测试工具平台</a>
        </div>
        <div id="navbar" class="navbar-collapse collapse">
          <ul class="nav navbar-nav">
            <li class="active"><a href="#"><span class="glyphicon glyphicon-home"></span>首页</a></li>
            <li class="dropdown">
              <a href="#" class="dropdown-toggle" data-toggle="dropdown" role="button" aria-haspopup="true" aria-expanded="false">
              <span class="glyphicon glyphicon-briefcase"></span> 测试辅助工具<span class="caret"></span></a>
              <ul class="dropdown-menu">
                <li><a href="javascript:changeTo('/toolkit/generatePic.do')"><span class="glyphicon glyphicon-picture"></span>  图片截取</a></li>
                <li><a href="javascript:changeTo('/toolkit/characterCount.do')"><span class="glyphicon glyphicon-eye-open"></span>  字数统计</a></li>
             </ul>
            </li>
          </ul>
        </div>
      </div>
    </nav>

	<div class="container" id="content">
	 	
	</div>
</div>
</body>
</html>
