/**
 * @author xujie-iri
 * @time 2016-7-15
 */

var selpdtline = "pdtsou"
var selectText = "trunk"
var selectEnv = "trunk"
var pdtLineText = "搜索";

$(document).ready(function() {
	$(".radioItem").change(function() {
		var selectedvalue = $("input[name='radioquota']:checked").val();
		if(selectedvalue == 'modepdt') {
			$("#divplan").hide();
			
		} else {
			$("#divplan").show();
		}
		
		selpdtline = $("input[name='radiobgttype']:checked").val();
		if(selpdtline == 'pdtbuer') {
			pdtLineText = "布尔"
			$("#labplan").show();
			$("#valueplan").show();
			$("#labgrp").hide();
			$("#valuegrp").hide();
			$("#labad").show();
			$("#valuead").show();
		}
		else if(selpdtline == 'pdtmv') {
			pdtLineText = "mv"
			$("#divplan").hide();
		}
		else if(selpdtline == 'pdtruyi'){
			pdtLineText = "ruyi"
			$("#labplan").show();
			$("#valueplan").show();
			$("#labgrp").show();
			$("#valuegrp").show();
			$("#labad").show();
			$("#valuead").show();
		}
		else {
			pdtLineText = "sou"
			$("#labplan").show();
			$("#valueplan").show();
			$("#labgrp").show();
			$("#valuegrp").show();
			$("#labad").show();
			$("#valuead").show();
			
//			$("#t").append("<tr><th>学号</th><th>姓名</th><th>年龄</th></tr>");
//			
//			list = [{"id":"1","name":"A","age":20},{"id":"2","name":"B","age":21},{"id":"3","name":"C","age":22}];
//			for(var index = 0,len = list.length; index<len; index++){
//			$("#quotadata tbody").append("<tr><td>"+ list[index].id +"</td><td>"+ list[index].name+"</td><td>"+ list[index].age +"</td></tr>");
//			}
			
		}
	});
	
	
	 $("#btnbgt").bind("click",
	     function() {
		 var selectedvalue = $("input[name='radioquota']:checked").val();
		 var uid=$("#uid").val();
		 if (selectedvalue == 'modepdt') {
			 if($.trim($("#uid").val())=="") {
			     alert('uid不能为空');
			 }
			 $.ajax({
					type: "POST",
					url: "/toolkit/budgetExhausted.do",
					data:{env:selectEnv,pdtline:selpdtline,uid:uid},
					dataType: 'json',
					cache: false,
					async:false,
					success: function(data, textStatus){
						//返回的data中有产品线状态（true or flase）、uid、xx_quota、yesterday_xx_cost、xx_cost、balance、cur_date、update_time、settle_date
//						if(data.status == "true"):
//							var msg = selectText + "环境，uid：" + uid + ",产品线: " + pdtLineText + "撞线成功！";
//							$("#bgtmsg")[0].innerHTML = msg;
//						else:
//							alert("uid:" + uid不存在);
					}
				});
		 }
		 
		 if (selectedvalue == 'modeplan') {
			 if($.trim($("#uid").val())=="") {
			     alert('uid不能为空');
			 }
			 else if($.trim($("#productlinebgt").val())=="") {
				 alert('预算值不能为空'); 
			 }
			 var budget = $("#productlinebgt").val();
			 $.ajax({
					type: "POST",
					url: "/toolkit/setProductLineBudget.do",
					data:{uid:uid, pdtline:selpdtline, budget:budget},
					dataType: 'json',
					cache: false,
					async:false,
					success: function(data, textStatus){
						$("#bgtmsg")[0].innerHTML='uid: ' + uid + ",产品线: " + selectText +  "的预算值是 " + data.budget;
//						alert('预算值' + data.budget);
					}
				});
		}
    });
});


function SelectChange() {
	//获取下拉框选中项的text属性值
	selectText = $("#selectenv").find("option:selected").text();
	if(selectText === "trunk") {
		selectEnv = 'trunk';
	}
	else {
		selectEnv = 'online';
	}	

//	alert(selectText);
}