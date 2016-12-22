
$(document).ready(function(e) {
	$('body').on('change',$('#ImportPicInput'),function(){
		$("#importPicName").val($("#ImportPicInput").val());
		
		$("#picSucResult").hide();
		$("#downLoadLink").attr("href","");
		$("#picFailResult").hide();
	});
});
/**
 * 判断上传文件类型是否合法
 * @returns {Boolean}
 */
function isImportFileTypeLegal(){
	
	var uploadFileName = $("#ImportPicInput").val();

    var last=uploadFileName.match(/^(.*)(\.)(.{1,8})$/)[3];   //获取文件格式
    last=last.toUpperCase();
    if(last != "PNG" && last != "JPG"){
    	alert("您上传文件类型不正确，请上传.jpg或.png的文件！");
    	return false;
    }
    return true;
}

/**
 * 生成图片
 */
function generatePics(){
	
	 var isPicParamLegal=true;
	 var text="[";
	 $('#PicParamTable').find('.PicParamInfo').each(function(name,value){
	        
	    	var width = $(value).find(".width").val();
	        var height = $(value).find(".height").val();
	        if(width=="" || height==""){
	        	isPicParamLegal=false;
	        }
	        var jpg = $(value).find(".jpg").is(':checked');
	        var png = $(value).find(".png").is(':checked');
	        var gif = $(value).find(".gif").is(':checked');
	        var bmp = $(value).find(".bmp").is(':checked');
	        
	        text += "{'width':'" + width + "',";
	        text += "'height':'" + height + "',";
	        text += "'jpg':'" + jpg + "',";
	        text += "'png':'" + png + "',";
	        text += "'gif':'" + gif + "',";
	        text += "'bmp':'" + bmp + "'}";
	        text += ",";
	 });
	 text = text.substring(0,text.length-1);
	 text += ']';
	 
	 
	 if($("#importPicName").val()==""){
		 alert("请选择要要上传的文件！");
	 }if(!isImportFileTypeLegal()){
		 
	 }else if(!isPicParamLegal){
		alert("宽和高信息不能为空,请重新修改！");
	 }else{
		$.ajaxFileUpload({
			type: "POST",
			url: "/toolkit/importPicFile.do",
			data:{picParams:text},
			secureuri : false,
	        fileElementId:'ImportPicInput',
	        dataType: 'json',
	        async : false,
			success: function(data){
				if(data.result=='success'){
					$("#picSucResult").show();
		        	$("#downLoadLink").attr("href","upload/"+data.filename);
				}else{
					$("#errorMsg").text(data.msg);
		        	$("#picFailResult").show();
				}
	        },
	        error: function (data, status, e){
	        	$("#errorMsg").text(data.msg);
	        	$("#picFailResult").show();
	        }
		});
	}
}

/**
 * 增加一行
 * @param e
 */
function addOneToPicParam(e){
	
	$("#PicParamTable").append($(e).parents(".PicParamInfo").clone());	
	$(".PicParamInfo:last").find(".width").val("");
	$(".PicParamInfo:last").find(".height").val("");
}

/**
 * 删除一行
 * @param e
 */
function deleteOneToPicParam(e){
	
	var len = $("#PicParamTable").find('tr').length;
	if (len==2) {
		alert("不能少于1行");
	}else {
		$(e).parents("tr").remove();
	}
}


