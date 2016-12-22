/**
 * @author xujie-iri
 * @time 2016-7-15
 */
/**
 * 统计字符数
 */
function getCharCount(){
	
	var originalContent=$("#originalContent").val();
	var chineseMulti=$("#chineseMulti").val();
	$.ajax({
		type: "POST",
		url: "/toolkit/getCharCount.do",
		contentType: "application/x-www-form-urlencoded; charset=utf-8",
		data:{originalContent:originalContent,chineseMulti:chineseMulti},
		dataType: 'json',
		cache: false,
		async:false,
		success: function(data, textStatus){
			if(data.result=="success"){
				$("#chinese").val(data.chinese);
				$("#english").val(data.english);
				$("#num").val(data.num);
				$("#space").val(data.space);
				$("#other").val(data.other);
				$("#total").val(data.total);
			}else{
				alert("解析内容出错，内容不能为空！");
			}
			
		}
	});
}
/**
 * 中文字计算方式变化后总数跟着变化
 */
function changeChMulti(){
	var chineseMulti=$("#chineseMulti").val();
	var chinese=$("#chinese").val();
	var english=$("#english").val();
	var num=$("#num").val();
	var space=$("#space").val();
	var other=$("#other").val();
	var total=parseInt(chinese)*parseInt(chineseMulti) + parseInt(english) + parseInt(num) + parseInt(space) + parseInt(other);
	$("#total").val(total);
}