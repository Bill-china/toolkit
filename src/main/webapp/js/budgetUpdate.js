/**
 * @author xujie-iri
 * @time 2016-7-15
 */
//function to_change(){
//	$('input[type=radio][name=radiopdtline]').change(function() {
//        if (this.value == 'modequery') {
//        	$("#divProductline").hide();
//        }else if (this.value == 'modeupdate') {
//        	$("#divProductline").show();
//        }
//    });
//}
var selpdtline = "sou"
var selectText = "搜索"
var selenv = "trunk"

$(document).ready(function() {
	$(".radioItem").change(function() {
		var selectedvalue = $("input[name='radiopdtline']:checked").val();
		if (selectedvalue == 'modequery') {
			$("#divProductline").hide();
		} else {
			$("#divProductline").show();
		}
	});
	
	$(".radioItem1").change(function() {
		var selectedvalue = $("input[name='radioplan']:checked").val();
		if (selectedvalue == 'modequery') {
			$("#divplan").hide();
		} else {
			$("#divplan").show();
		}
	});
	
	 $("#btnpdtline").bind("click",
	     function() {
		 var selectedvalue = $("input[name='radiopdtline']:checked").val();
		 var uid=$("#uid").val();
		 if (selectedvalue == 'modequery') {
			 if($.trim($("#uid").val())=="") {
			     alert('uid不能为空');
			 }
			 $.ajax({
					type: "POST",
					url: "/toolkit/getProductLineBudget.do",
					data:{uid:uid,pdtline:selpdtline,env:selenv},
					dataType: 'json',
					cache: false,
					async:false,
					success: function(data, textStatus){
						$("#bgtmsg")[0].innerHTML='uid: ' + uid + ",产品线: " + selectText +  "的预算值是 " + data.budget;
//						alert('预算值' + data.budget);
					}
				});
		 }
		 
		 if (selectedvalue == 'modeupdate') {
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
					data:{uid:uid, pdtline:selpdtline, env:selenv, budget:budget},
					dataType: 'json',
					cache: false,
					async:false,
					success: function(data, textStatus){
						if(data.errno == "0")
							alert('预算值更新成功');
						else
							alert('预算值更新失败');
					}
				});
		}
    });
	 
	 $("#btnplanquota").bind("click",
		     function() {
			 var selectedvalue = $("input[name='radioplan']:checked").val();
			 var uid=$("#uid").val();
			 if (selectedvalue == 'modequery') {
				 if($.trim($("#pid").val())=="") {
				     alert('pid不能为空');
				 }
				 if($.trim($("#uid").val())=="") {
				     alert('uid不能为空');
				 }
				 $.ajax({
						type: "POST",
						url: "/toolkit/getPlanBudget.do",
						data:{uid:uid,pdtline:selpdtline,env:selenv},
						dataType: 'json',
						cache: false,
						async:false,
						success: function(data, textStatus){
							$("#bgtmsg")[0].innerHTML='uid: ' + uid + ",产品线: " + selectText +  "的预算值是 " + data.budget;
//							alert('预算值' + data.budget);
						}
					});
			 }
			 
			 if (selectedvalue == 'modeupdate') {
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
						data:{uid:uid, pdtline:selpdtline, env:selenv, budget:budget},
						dataType: 'json',
						cache: false,
						async:false,
						success: function(data, textStatus){
							if(data.errno == "0")
								alert('预算值更新成功');
							else
								alert('预算值更新失败');
						}
					});
			}
	    });
	 
	 
});


function SelectChange() {
	//获取下拉框选中项的text属性值
	selectText = $("#selectpdtline").find("option:selected").text();
	if(selectText === "搜索") {
		selpdtline = 'sou';
	}
	else if(selectText === "布尔") {
		selpdtline = 'buer';
	}
	else if(selectText === "mv") {
		selpdtline = 'mv';
	}
	else
		selpdtline = 'ruyi';
	
	selenv = $("#selectenv").find("option:selected").text();

//	alert(selectText);
}