function fileQueueError(file, errorCode, message) {
    try {
        if (errorCode === SWFUpload.QUEUE_ERROR.QUEUE_LIMIT_EXCEEDED) {
            alert("您正在上传的文件队列过多.\n" + (message === 0 ? "您已达到上传限制" : "您最多能选择 " + (message > 1 ? "上传 " + message + " 文件." : "一个文件.")));
            return;
        }
        var upload_info=this.customSettings.progressTarget;
        var progress = new FileProgress(file, this.customSettings.progressTarget);
        progress.setError();
        progress.toggleCancel(false);
        $('#'+upload_info).show();
        switch (errorCode) {
        case SWFUpload.QUEUE_ERROR.FILE_EXCEEDS_SIZE_LIMIT:
            progress.setStatus("文件过大, 文件大小: " + Math.round(file.size/1024)+"k");
            $('#'+upload_info).fadeOut(5000);
            this.debug("错误代码: 文件过大, 文件名: " + file.name + ", 文件大小: " + file.size + ", 信息: " + message);
            break;
        case SWFUpload.QUEUE_ERROR.ZERO_BYTE_FILE:
            progress.setStatus("无法上传零字节文件.");
            this.debug("错误代码: 零字节文件, 文件名: " + file.name + ", 文件大小: " + file.size + ", 信息: " + message);
            $('#'+upload_info).fadeOut(5000);
            break;
        case SWFUpload.QUEUE_ERROR.INVALID_FILETYPE:
            progress.setStatus("不支持的文件类型.");
            $('#'+upload_info).fadeOut(5000);
            this.debug("错误代码: 不支持的文件类型, 文件名: " + file.name + ", 文件大小: " + file.size + ", 信息: " + message);
            break;
        default:
            if (file !== null) {
                progress.setStatus("未处理的错误");
                $('#'+upload_info).fadeOut(5000);
            }
            this.debug("错误代码: " + errorCode + ", 文件名: " + file.name + ", 文件大小: " + file.size + ", 信息: " + message);
            break;
        }
    } catch (ex) {
        this.debug(ex);
    }
}

function fileDialogComplete(numFilesSelected, numFilesQueued) {
    try {
        if (numFilesQueued > 0) {
            this.startUpload();
        }
    } catch (ex) {
        this.debug(ex);
    }
}

function uploadProgress(file, bytesLoaded) {

    try {
        var percent = Math.ceil((bytesLoaded / file.size) * 100);
        var progress = new FileProgress(file,  this.customSettings.progressTarget);
        progress.setProgress(percent);
        if (percent === 100) {
            progress.setStatus("缩略图创建中...");
            progress.toggleCancel(false, this);
        } else {
            progress.setStatus("上传中...");
            progress.toggleCancel(true, this);
        }
    } catch (ex) {
        this.debug(ex);
    }
}

function uploadSuccess(file, serverData) {
    try {
        var upload_info=this.customSettings.progressTarget;
        var upload_input=this.customSettings.uploadTarget;
        var progress = new FileProgress(file, this.customSettings.progressTarget);
        var arrayServerData=serverData.split(',');
        var upload_type=arrayServerData[0],src=arrayServerData[1],adv_size=arrayServerData[2],suc_adv_width=adv_size.split("x")[0],suc_adv_height=adv_size.split("x")[1];
        var width=$('#'+upload_info+'_preview').attr('width'),height=$('#'+upload_info+'_preview').attr('height');
        var atr_width=$('#'+upload_input).attr("sizew"),atr_height=$('#'+upload_input).attr("sizeh");
        $('#'+upload_info).show();
        if(upload_type == 'swf'){
            progress.setComplete();
            progress.toggleCancel(true);
            progress.setStatus("上传成功");
            $('#'+upload_input).val(src);
            $('#'+upload_info).html('<embed wmode="transparent" quality="high" pluginspage="http://www.macromedia.com/go/getflashplayer" type="application/x-shockwave-flash" src="'+src+'" width="'+width+'" height="'+height+'" />');
        }
        else if(upload_type == 'img'){
            if((atr_width == suc_adv_width && atr_height == suc_adv_height) || (! atr_width && ! atr_height)){
            progress.setComplete();
            //progress.toggleCancel(true);
            progress.setStatus("上传成功");
            $('#'+upload_input).val(src);
            $('#'+upload_info).html('<img src="'+src+'" width="'+suc_adv_width+'" height="'+suc_adv_height+'" />');
            }else{
                alert("图片尺寸错误");
                progress.setComplete();
                progress.toggleCancel(false);
                progress.setStatus("图片尺寸错误, 图片尺寸: " + suc_adv_width+"*"+suc_adv_height);
                $('#'+upload_info).fadeOut(5000);
            }
        }
        else{
            progress.setComplete();
            progress.toggleCancel(false,this);
            progress.setStatus("未知类型错误");
        }
    } catch (ex) {
        this.debug(ex);
    }
}

function uploadComplete(file) {
    try {
        /*  I want the next upload to continue automatically so I'll call startUpload here */
        if (this.getStats().files_queued > 0) {
            this.startUpload();
        } else {
            var progress = new FileProgress(file,  this.customSettings.progressTarget);
            progress.setComplete();
            //progress.setStatus("图片上传完成！");
            progress.toggleCancel(false);
        }
    } catch (ex) {
        this.debug(ex);
    }
}

function uploadError(file, errorCode, message) {
    var imageName =  "error.gif";
    var progress;
    try {
        switch (errorCode) {
        case SWFUpload.UPLOAD_ERROR.FILE_CANCELLED:
            try {
                progress = new FileProgress(file,  this.customSettings.progressTarget);
                progress.setCancelled();
                progress.setStatus("Cancelled");
                progress.toggleCancel(false);
            }
            catch (ex1) {
                this.debug(ex1);
            }
            break;
        case SWFUpload.UPLOAD_ERROR.UPLOAD_STOPPED:
            try {
                progress = new FileProgress(file,  this.customSettings.progressTarget);
                progress.setCancelled();
                progress.setStatus("Stopped");
                progress.toggleCancel(true);
            }
            catch (ex2) {
                this.debug(ex2);
            }
        case SWFUpload.UPLOAD_ERROR.UPLOAD_LIMIT_EXCEEDED:
            imageName = "uploadlimit.gif";
            break;
        default:
            alert(message);
            break;
        }
    } catch (ex3) {
        this.debug(ex3);
    }

}

function addSwf(src,domimg) {
    var newImg = document.getElementById(domimg);
    newImg.style.display = "";
    newImg.style.margin = "0";

    if (newImg.filters) {
        try {
            newImg.filters.item("DXImageTransform.Microsoft.Alpha").opacity = 0;
        } catch (e) {
            // If it is not set initially, the browser will throw an error.  This will set it if it is not set yet.
            newImg.style.filter = 'progid:DXImageTransform.Microsoft.Alpha(opacity=' + 0 + ')';
        }
    } else {
        newImg.style.opacity = 0;
    }

    newImg.onload = function () {
        fadeIn(newImg, 0);
    };
    newImg.src = src;
}
function addImage(src,domimg) {
    var newImg = document.getElementById(domimg);
    newImg.style.display = "";
    newImg.style.margin = "0";

    if (newImg.filters) {
        try {
            newImg.filters.item("DXImageTransform.Microsoft.Alpha").opacity = 0;
        } catch (e) {
            // If it is not set initially, the browser will throw an error.  This will set it if it is not set yet.
            newImg.style.filter = 'progid:DXImageTransform.Microsoft.Alpha(opacity=' + 0 + ')';
        }
    } else {
        newImg.style.opacity = 0;
    }

    newImg.onload = function () {
        fadeIn(newImg, 0);
    };
    newImg.src = src;
}

function fadeIn(element, opacity) {
    var reduceOpacityBy = 5;
    var rate = 30;	// 15 fps
    if (opacity < 100) {
        opacity += reduceOpacityBy;
        if (opacity > 100) {
            opacity = 100;
        }

        if (element.filters) {
            try {
                element.filters.item("DXImageTransform.Microsoft.Alpha").opacity = opacity;
            } catch (e) {
                // If it is not set initially, the browser will throw an error.  This will set it if it is not set yet.
                element.style.filter = 'progid:DXImageTransform.Microsoft.Alpha(opacity=' + opacity + ')';
            }
        } else {
            element.style.opacity = opacity / 100;
        }
    }

    if (opacity < 100) {
        setTimeout(function () {
            fadeIn(element, opacity);
        }, rate);
    }
}



/* ******************************************
 *	FileProgress Object
 *	Control object for displaying file info
 * ****************************************** */

function FileProgress(file, targetID) {
    this.fileProgressID = "divFileProgress";

    this.fileProgressWrapper = document.getElementById(this.fileProgressID);
    if (!this.fileProgressWrapper) {
        this.fileProgressWrapper = document.createElement("div");
        this.fileProgressWrapper.className = "progressWrapper";
        this.fileProgressWrapper.id = this.fileProgressID;

        this.fileProgressElement = document.createElement("div");
        this.fileProgressElement.className = "progressContainer";

        var progressCancel = document.createElement("a");
        progressCancel.className = "progressCancel";
        progressCancel.href = "#";
        progressCancel.style.visibility = "hidden";
        progressCancel.appendChild(document.createTextNode(" "));

        var progressText = document.createElement("div");
        progressText.className = "progressName";
        progressText.appendChild(document.createTextNode(file.name));

        var progressBar = document.createElement("div");
        progressBar.className = "progressBarInProgress";

        var progressStatus = document.createElement("div");
        progressStatus.className = "progressBarStatus";
        progressStatus.innerHTML = "&nbsp;";

        this.fileProgressElement.appendChild(progressCancel);
        this.fileProgressElement.appendChild(progressText);
        this.fileProgressElement.appendChild(progressStatus);
        this.fileProgressElement.appendChild(progressBar);

        this.fileProgressWrapper.appendChild(this.fileProgressElement);

        document.getElementById(targetID).appendChild(this.fileProgressWrapper);
        fadeIn(this.fileProgressWrapper, 0);

    } else {
        this.fileProgressElement = this.fileProgressWrapper.firstChild;
        this.fileProgressElement.childNodes[1].firstChild.nodeValue = file.name;
    }

    this.height = this.fileProgressWrapper.offsetHeight;

}
FileProgress.prototype.setProgress = function (percentage) {
    this.fileProgressElement.className = "progressContainer green";
    this.fileProgressElement.childNodes[3].className = "progressBarInProgress";
    this.fileProgressElement.childNodes[3].style.width = percentage + "%";
};
FileProgress.prototype.setComplete = function () {
    this.fileProgressElement.className = "progressContainer blue";
    this.fileProgressElement.childNodes[3].className = "progressBarComplete";
    this.fileProgressElement.childNodes[3].style.width = "";

};
FileProgress.prototype.setError = function () {
    this.fileProgressElement.className = "progressContainer red";
    this.fileProgressElement.childNodes[3].className = "progressBarError";
    this.fileProgressElement.childNodes[3].style.width = "";

};
FileProgress.prototype.setCancelled = function () {
    this.fileProgressElement.className = "progressContainer";
    this.fileProgressElement.childNodes[3].className = "progressBarError";
    this.fileProgressElement.childNodes[3].style.width = "";

};
FileProgress.prototype.setStatus = function (status) {
    this.fileProgressElement.childNodes[2].innerHTML = status;
};

FileProgress.prototype.toggleCancel = function (show, swfuploadInstance) {
    this.fileProgressElement.childNodes[0].style.visibility = show ? "visible" : "hidden";
    if (swfuploadInstance) {
        var fileID = this.fileProgressID;
        this.fileProgressElement.childNodes[0].onclick = function () {
            swfuploadInstance.cancelUpload(fileID);
            return false;
        };
    }
};
