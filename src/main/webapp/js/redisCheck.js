/**
 * @author xujie-iri
 * @time 2016-7-15
 */

var monitorTimer;//必须设置为全局变量
var editorList=[window.editor,window.editor1,window.editor2,window.editor3];
$(document).ready(function() {
	
	$("#tabs").tabs();
	//测试环境的切换代码
    $('input[type=radio][name=env]').change(function() {
        if (this.value == 'trunk') {
        	$("#redisPort").val("17801/17802");
        }else if (this.value == 'online') {
        	$("#redisPort").val("17803/17804");
        }
    });
    //日志展示区用的codemirror，这里是codemirror的配置
    var jsonList=["pv_redis","pv_combineLog","click_redis","click_cheatLog"];
    for(var i=0;i<4;i++){
    	editorList[i] = CodeMirror.fromTextArea(document.getElementById(jsonList[i]), {
    	    lineNumbers: true,
    	    lineWrapping: true,
    	    theme: "dracula",
    	    extraKeys: {
    	      "Ctrl-Q": function(cm){
    	    	  cm.foldCode(cm.getCursor()); 
    	      },
    	      "Alt-F": "findPersistent"
    	    },
    		foldGutter: true,
    		gutters: ["CodeMirror-linenumbers", "CodeMirror-foldgutter"]
    	});
    }
   
});

/***
 * redis日志监控服务
 */
function monitorService(){
	
	var monitorState=$("#monitorServiceButton").val();
	if(monitorState=="启动监控服务"){
		monitorTimer=setInterval(getMonitorContent,2000);
		startMonitorService();
	}else if(monitorState=="停止监控服务"){
		window.clearInterval(monitorTimer);
		stopMonitorService();
	}
	return false;//注意在form提交，必须写return false;
}

/***
 * 启动redis日志监控服务
 */
function startMonitorService(){
	
	var redisHostIp=$("#redisHostIp").val();
	var redisPort=$("#redisPort").val();
	var checkValue=$("#checkValue").val();
	var fileHostIp=$("#fileHostIp").val();
	var filePort=$("#filePort").val();
	$.ajax({
		type: "POST",
		url: "/toolkit/startMonitorService.do",
		data:{
			fileHostIp:fileHostIp,
			filePort:filePort,
			redisHostIp:redisHostIp,
			redisPort:redisPort,
			checkValue:checkValue
			},
		cache: false,
		async:false,
		success: function(data, textStatus){
			$("#monitorServiceButton").val("停止监控服务");
		}
	});
}


/**
 *  获取redis监听内容
 */
function getMonitorContent(){
	var fileHostIp=$("#fileHostIp").val();
	var filePort=$("#filePort").val();
	var pv_latesttime = $("#pv_latesttime").val();
	var click_latesttime = $("#click_latesttime").val();
	$.ajax({
		type: "POST",
		url: "/toolkit/getMonitorContent.do",
		data:{
			fileHostIp:fileHostIp,
			filePort:filePort,
			pv_latesttime:pv_latesttime,
			click_latesttime:click_latesttime},
		dataType: 'json',
		cache: false,
		async:false,
		success: function(data, textStatus){
			if(data.type=="view" && data.redisLog!="noMoreNew"){
				$("#tabs").find("a").eq(0).click();
				var rLog= jQuery.parseJSON(data.redisLog);
				var formatJson=JSON.stringify(rLog,undefined, 2);
				editorList[0].setValue(formatJson);
				$("#pv_latesttime").val(data.latesttime);
				
				$("#tabs").find("a").eq(1).click();
				var combineLog=parseCombineLog(data.combineLog);
				var cLog= jQuery.parseJSON(combineLog);
				var combJson=JSON.stringify(cLog,undefined, 2);
				editorList[1].setValue(combJson);
			}else if(data.type=="click" && data.redisLog!="noMoreNew"){
				$("#tabs").find("a").eq(2).click();
				var rLog= jQuery.parseJSON(data.redisLog);
				var formatJson=JSON.stringify(rLog,undefined, 2);
				editorList[2].setValue(formatJson);
				$("#click_latesttime").val(data.latesttime);
				
				$("#tabs").find("a").eq(3).click();
				editorList[3].setValue(data.cheatClick);
				editorList[3].refresh();
			}
		}
	});
}


function getYear(){
	return new Date().getFullYear();
}

/**
 * 解析combineLog
 */
function parseCombineLog(combineLog){
	var result="";
	var reg=new RegExp(getYear()+"\\d+\\s+Dj\\w+Pv","g");
	var combList=combineLog.split(reg);
	if(combList.length>2){
		for(var i=1;i<combList.length;i++){
			result+="\"comb"+i+"\":"+combList[i].trim();
		}
		result="{"+result.substring(0,result.length-1)+"}"
	}else{
		result=combList[1].trim();
	}
	return result;
}

/***
 * 停止redis日志监控服务
 */
function stopMonitorService(){
	
	$.ajax({
		type: "POST",
		url: "/toolkit/stopMonitorService.do",
		data:{},
		cache: false,
		async:false,
		success: function(data, textStatus){
			$("#monitorServiceButton").val("启动监控服务");
		}
	});
}


