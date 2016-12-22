/**
 * @author xujie-iri
 * @time 2016-7-15
 */

var selpdtline = "sou"
var selectText = "搜索"

$(document).ready(function() {
	$(".radioItem").change(function() {
		var selectedvalue = $("input[name='radiobalance']:checked").val();
		if (selectedvalue == 'modequery') {
			$("#divbalance").hide();
		} else {
			$("#divbalance").show();
		}
	});
	
	
	 $("#btnbalance").bind("click",
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
					data:{uid:uid,pdtline:selpdtline},
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
//	alert(selectText);
}