function swfupload(s_parent, custom_settings) {
    var settings = {
        flash_url: "scripts/swfupload.swf",
        upload_url: "upload/uploadimg.php",
        file_size_limit: "2 MB",
        post_params: {},
        file_types: "*.png;*.jpg;*.gif;",
        file_types_description: "不超过2M的图片",
        file_upload_limit: 3,
        file_queue_limit: 1,
        custom_settings: {
            progressTarget: "flashUploadProgress"
        },
        // Button  settings
        button_image_url: '/images/btn_upload.png',
        button_width: "55",
        button_height: "26",
        button_placeholder_id: "flashButtonPlaceholder",
        //button_text : '<span class="btnText">上传</span>',
        button_text_style: ".btnText { color:#0000ff;  font-size:12px;text-align:center; font-weight:bold; }",
        button_action: SWFUpload.BUTTON_ACTION.SELECT_FILE,
        button_cursor: SWFUpload.CURSOR.HAND,
        // The event handler functions are  defined in handlers.js                                                                
        swfupload_loaded_handler: swfUploadLoaded,
        file_queued_handler: fileQueued,
        file_queue_error_handler: fileQueueError,
        file_dialog_complete_handler: fileDialogComplete,
        upload_start_handler: uploadStart,
        upload_progress_handler: uploadProgress,
        upload_error_handler: uploadError,
        upload_success_handler: uploadSuccess,
        upload_type: "file",
        file_width: 0,

        // SWFObject  settings                                                                                                    
        minimum_flash_version: "9.0.28",
        swfupload_pre_load_handler: swfUploadPreLoad,
        swfupload_load_failed_handler: swfUploadLoadFailed
    };

    settings = $.fn.extend(settings, custom_settings);
    var ids = {
        span_s_info: ' .span_s_info',
        input_key: ' .input_key_t',
        input_name: ' .input_name_t',
        img: ' .uploadimg'
    };

    function upload_ok(name, key) {
        var eles = {};
        var i;
        for (i in ids) {
            eles[i] = $(s_parent + ids[i]);
        }
        eles['input_name'].val(name);
        eles['input_key'].val(key);

        var type = settings['upload_type'];
        var width = settings['file_width'];
        var filename = $(s_parent + " .input_name_t").val();
        var index = filename.lastIndexOf(".");
        var suffix = filename.substring(index + 1);
        if(!settings.showsub){
            $(settings.boxid + ' sub').hide();
        }
        W(settings.boxid + ' input[type="text"]~.error').hide();
        switch (type) {
        case "logo":
            updateLogo(width);
            break;
        case "exelogo":
            updateExeLogo(width);
            break;
        case "shot":
            $(s_parent+' .uploadimg').attr('src', "show.php?key=" + $(s_parent + " .input_key_t").val() + '&suffix=' + suffix);
            $(s_parent+' .input_key_t').val("show.php?key=" + $(s_parent + " .input_key_t").val() + '&suffix=' + suffix);
            break;
        case "exe":
            $('#exe_review').attr('href', 'show.php?key=' + $(s_parent + " .input_key_t").val() + '&suffix=' + suffix);
            break;
        case "flash":
            $('#flash_review').attr('href', 'show.php?key=' + $(s_parent + " .input_key_t").val() + '&suffix=' + suffix);
            break;
        case "bg":
            $('#bg_review').attr('href', 'show.php?key=' + $(s_parent + " .input_key_t").val() + '&suffix=' + suffix);
            break;
        case "license":
            $('#license_review').attr('href', 'show.php?key=' + $(s_parent + " .input_key_t").val() + '&suffix=' + suffix);
        default:
            break;
        }

    }

    function swfUploadPreLoad() {
        var a = this;
        this.customSettings.loadingTimeout = setTimeout(function() {
            $(s_parent + " .flashLoadingContent").show();
            a.customSettings.loadingTimeout = setTimeout(function() {
                $(s_parent + " .flashLoadingContent").hide();
               // $(s_parent + " .flashLongLoading").show()
                $(s_parent + " .flashLoadingTimeout").show()
            },
            30 * 1000)
        },
        1 * 1000)
    }
    function swfUploadLoaded() {
        var a = this;
        clearTimeout(this.customSettings.loadingTimeout);
        $(s_parent + " .flashLoadingContent").hide();
        $(s_parent + " .flashLoadingTimeout").hide();
        $(s_parent + " .flashAlternateContent").hide()
    }
    function swfUploadLoadFailed(e) {
        clearTimeout(this.customSettings.loadingTimeout);
        $(s_parent + " .flashLoadingContent").hide();
        $(s_parent + " .flashLoadingTimeout").hide();
        $(s_parent + " .flashAlternateContent").show()
    }
    function uploadSuccess(d, b, e) {
        b = jQuery.trim(b);
        try {
            if (b.substring(0, 5) === "name=") {
                var a = new FileProgress(d, this.customSettings.progressTarget);
                a.setComplete();
                a.setStatus("\u4e0a\u4f20\u5e94\u7528\u6587\u4ef6\u6210\u529f.");
                a.toggleCancel(false);
                upload_ok(b.substring(5))
            }
            else {
                if (b.substring(0, 4) === "key=") {
                    var a = new FileProgress(d, this.customSettings.progressTarget);
                    a.setComplete();
                    a.setStatus("\u4e0a\u4f20\u6587\u4ef6\u6210\u529f.");
                    a.toggleCancel(false);
                    var f = b.substring(4).split("|||");

                    upload_ok(f[1], f[0])
                }
                else {
                    if (b == "status=-1") {
                        this.uploadError(d, "c_302", "\u672a\u767b\u5f55\u6216\u8005\u6ca1\u8865\u5145\u7528\u6237\u8d44\u6599")
                    }
                    else {
                        this.uploadError(d, "c_404", "\u4e0a\u4f20\u5931\u8d25")
                    }
                }
            }
        }
        catch(c) {
            this.debug(c);
        }
    }
    function updateLogo(width) {
        var m7 = W('.m7');
        if (width == 0) {
            update_logo();
            document.getElementById('uparea').style.display = 'block';
            m7.css('height', parseInt(m7.css('height')) + 160 + 'px');
        }
        else {
                suffix = "png";
                update_logo_by_width(s_parent + " .input_key_t", s_parent + " .uploadimg", suffix);

        }

    }
    function updateExeLogo(width) {
        var m7 = W('.m7');
        if (width == 0) {
            update_exelogo();
            document.getElementById('uparea').style.display = 'block';
            m7.css('height', parseInt(m7.css('height')) + 160 + 'px');
        }
        else {
                suffix = "png";
                update_logo_by_width(s_parent + " .input_key_t", s_parent + " .uploadimg", suffix);

        }

    }
    return new SWFUpload(settings);
}

function update_logo() {
    var logokey = $("#upload_web_logo .input_key_t").val();
    var logoname = $("#upload_web_logo .input_name_t").val();
    var logokeyarr = logokey.split(',');
    $('#upload_web_logo72 .input_key_t').val(logokeyarr[0]);
    $('#upload_web_logo64 .input_key_t').val(logokeyarr[1]);
    $('#upload_web_logo48 .input_key_t').val(logokeyarr[2]);
    $('#upload_web_logo32 .input_key_t').val(logokeyarr[3]);
    $('#upload_web_logo16 .input_key_t').val(logokeyarr[4]);
    var index = logoname.lastIndexOf(".");
    var suffix = logoname.substring(index);
    var name = logoname.substring(0, index);

    $('#upload_web_logo72 .input_name_t').val(name + "_72" + suffix);
    $('#upload_web_logo64 .input_name_t').val(name + "_64" + suffix);
    $('#upload_web_logo48 .input_name_t').val(name + "_48" + suffix);
    $('#upload_web_logo32 .input_name_t').val(name + "_32" + suffix);
    $('#upload_web_logo16 .input_name_t').val(name + "_16" + suffix);

    $('#upload_web_logo64 .uploadimg').attr('src', '/show.php?key=' + logokeyarr[1] + '&suffix=png');
    $('#upload_web_logo48 .uploadimg').attr('src', '/show.php?key=' + logokeyarr[2] + '&suffix=png');
    $('#upload_web_logo32 .uploadimg').attr('src', '/show.php?key=' + logokeyarr[3] + '&suffix=png');
    $('#upload_web_logo16 .uploadimg').attr('src', '/show.php?key=' + logokeyarr[4] + '&suffix=png');
}
function update_exelogo() {
    var logokey = $("#upload_exe_logo .input_key_t").val();
    var logoname = $("#upload_exe_logo .input_name_t").val();
    var logokeyarr = logokey.split(',');
    $('#upload_exe_logo72 .input_key_t').val(logokeyarr[0]);
    $('#upload_exe_logo64 .input_key_t').val(logokeyarr[1]);
    $('#upload_exe_logo48 .input_key_t').val(logokeyarr[2]);
    $('#upload_exe_logo32 .input_key_t').val(logokeyarr[3]);
    $('#upload_exe_logo16 .input_key_t').val(logokeyarr[4]);
    var index = logoname.lastIndexOf(".");
    var suffix = logoname.substring(index);
    var name = logoname.substring(0, index);

    $('#upload_exe_logo72 .input_name_t').val(name + "_72" + suffix);
    $('#upload_exe_logo64 .input_name_t').val(name + "_64" + suffix);
    $('#upload_exe_logo48 .input_name_t').val(name + "_48" + suffix);
    $('#upload_exe_logo32 .input_name_t').val(name + "_32" + suffix);
    $('#upload_exe_logo16 .input_name_t').val(name + "_16" + suffix);

    $('#upload_exe_logo64 .uploadimg').attr('src', '/show.php?key=' + logokeyarr[1] + '&suffix=png');
    $('#upload_exe_logo48 .uploadimg').attr('src', '/show.php?key=' + logokeyarr[2] + '&suffix=png');
    $('#upload_exe_logo32 .uploadimg').attr('src', '/show.php?key=' + logokeyarr[3] + '&suffix=png');
    $('#upload_exe_logo16 .uploadimg').attr('src', '/show.php?key=' + logokeyarr[4] + '&suffix=png');
}

function update_logo_by_width(keyid, imgid, suffix) {
    var logokey = $(keyid).val();
    $(imgid).attr('src', '/show.php?key=' + logokey + '&suffix=' + suffix);
}

function fileQueued(c) {
    try {
        var a = new FileProgress(c, this.customSettings.progressTarget);
        a.setStatus("\u51c6\u5907\u4e2d...");
        a.toggleCancel(true, this)
    }
    catch(b) {
        this.debug(b)
    }
}
function fileQueueError(c, e, d) {
    try {
        if (e === SWFUpload.QUEUE_ERROR.QUEUE_LIMIT_EXCEEDED) {
            alert("\u60a8\u4e0a\u4f20\u7684\u592a\u9891\u7e41\u4e86\uff01");
            return;
        }
        var a = new FileProgress(c, this.customSettings.progressTarget);
        a.setError();
        a.toggleCancel(false);
        switch (e) {
        case SWFUpload.QUEUE_ERROR.FILE_EXCEEDS_SIZE_LIMIT:
            a.setStatus("\u6587\u4ef6\u592a\u5927\u4e86");
            this.debug("Error Code: File too big, File name: " + c.name + ", File size: " + c.size + ", Message: " + d);
            break;
        case SWFUpload.QUEUE_ERROR.ZERO_BYTE_FILE:
            a.setStatus("\u4e0d\u80fd\u4e0a\u4f200\u5b57\u8282\u7684\u6587\u4ef6");
            this.debug("Error Code: Zero byte file, File name: " + c.name + ", File size: " + c.size + ", Message: " + d);
            break;
        case SWFUpload.QUEUE_ERROR.INVALID_FILETYPE:
            a.setStatus("\u9519\u8bef\u7684\u6587\u4ef6\u7c7b\u578b");
            this.debug("Error Code: Invalid File Type, File name: " + c.name + ", File size: " + c.size + ", Message: " + d);
            break;
        default:
            if (c !== null) {
                a.setStatus("\u672a\u77e5\u9519\u8bef")
            }
            this.debug("Error Code: " + e + ", File name: " + c.name + ", File size: " + c.size + ", Message: " + d);
            break
        }
    }
    catch(b) {
        this.debug(b);
    }
}
function fileDialogComplete(a, c) {
    try {
        this.startUpload()
    }
    catch(b) {
        this.debug(b)
    }
}
function uploadStart(c) {
    try {
        var a = new FileProgress(c, this.customSettings.progressTarget);
        a.setStatus("\u4e0a\u4f20\u4e2d...");
        a.toggleCancel(true, this)
    }
    catch(b) {}
    return true
}
function uploadProgress(c, f, e) {
    try {
        var d = Math.ceil((f / e) * 100);
        var a = new FileProgress(c, this.customSettings.progressTarget);
        a.setProgress(d);
        a.setStatus("\u4e0a\u4f20\u4e2d...")
    }
    catch(b) {
        this.debug(b)
    }
}
function uploadError(c, e, d) {
    try {
        var a = new FileProgress(c, this.customSettings.progressTarget);
        a.setError();
        a.toggleCancel(false);
        switch (e) {
        case SWFUpload.UPLOAD_ERROR.HTTP_ERROR:
            a.setStatus("\u4e0a\u4f20\u51fa\u9519: " + d);
            this.debug("Error Code: HTTP Error, File name: " + c.name + ", Message: " + d);
            break;
        case SWFUpload.UPLOAD_ERROR.UPLOAD_FAILED:
            a.setStatus("\u4e0a\u4f20\u5931\u8d25.");
            this.debug("Error Code: Upload Failed, File name: " + c.name + ", File size: " + c.size + ", Message: " + d);
            break;
        case SWFUpload.UPLOAD_ERROR.IO_ERROR:
            a.setStatus("\u4e0a\u4f20\u51fa\u9519\uff1aServer (IO) Error");
            this.debug("Error Code: IO Error, File name: " + c.name + ", Message: " + d);
            break;
        case SWFUpload.UPLOAD_ERROR.SECURITY_ERROR:
            a.setStatus("\u4e0a\u4f20\u51fa\u9519\uff1aSecurity Error");
            this.debug("Error Code: Security Error, File name: " + c.name + ", Message: " + d);
            break;
        case SWFUpload.UPLOAD_ERROR.UPLOAD_LIMIT_EXCEEDED:
            a.setStatus("\u4e0a\u4f20\u8d85\u8fc7\u9650\u5236.");
            this.debug("Error Code: Upload Limit Exceeded, File name: " + c.name + ", File size: " + c.size + ", Message: " + d);
            break;
        case SWFUpload.UPLOAD_ERROR.FILE_VALIDATION_FAILED:
            a.setStatus("\u6587\u4ef6\u6821\u9a8c\u5931\u8d25\uff0c\u505c\u6b62\u4e0a\u4f20");
            this.debug("Error Code: File Validation Failed, File name: " + c.name + ", File size: " + c.size + ", Message: " + d);
            break;
        case SWFUpload.UPLOAD_ERROR.FILE_CANCELLED:
            a.setStatus("\u5df2\u7ecf\u53d6\u6d88");
            a.setCancelled();
            break;
        case SWFUpload.UPLOAD_ERROR.UPLOAD_STOPPED:
            a.setStatus("\u5df2\u7ecf\u505c\u6b62");
            break;
        case "c_404":
            a.setStatus(d);
            break;
        case "c_302":
            a.setStatus(d);
            break;
        default:
            a.setStatus("\u672a\u77e5\u9519\u8bef: " + e);
            this.debug("Error Code: " + e + ", File name: " + c.name + ", File size: " + c.size + ", Message: " + d);
            break
        }

    }
    catch(b) {
        this.debug(b)
    }

}
function FileProgress(c, a) {
    this.fileProgressID = c.id;
    this.opacity = 100;
    this.height = 0;
    this.fileProgressWrapper = document.getElementById(this.fileProgressID);
    if (!this.fileProgressWrapper) {
        this.fileProgressWrapper = document.createElement("div");
        this.fileProgressWrapper.className = "progressWrapper";
        this.fileProgressWrapper.id = this.fileProgressID;
        this.fileProgressElement = document.createElement("div");
        this.fileProgressElement.className = "progressContainer";
        var f = document.createElement("a");
        f.className = "progressCancel";
        f.href = "#";
        f.style.visibility = "hidden";
        f.appendChild(document.createTextNode(" "));
        var b = document.createElement("div");
        b.className = "progressName";
        b.appendChild(document.createTextNode(c.name));
        var e = document.createElement("div");
        e.className = "progressBarInProgress";
        var d = document.createElement("div");
        d.className = "progressBarStatus";
        d.innerHTML = "&nbsp;";
        this.fileProgressElement.appendChild(f);
        this.fileProgressElement.appendChild(b);
        this.fileProgressElement.appendChild(d);
        this.fileProgressElement.appendChild(e);
        this.fileProgressWrapper.appendChild(this.fileProgressElement);
        document.getElementById(a).appendChild(this.fileProgressWrapper)
    }
    else {
        this.fileProgressElement = this.fileProgressWrapper.firstChild;
        this.reset()
    }
    this.height = this.fileProgressWrapper.offsetHeight;
    this.setTimer(null)
}
FileProgress.prototype.setTimer = function(a) {
    this.fileProgressElement.FP_TIMER = a
};
FileProgress.prototype.getTimer = function(a) {
    return this.fileProgressElement.FP_TIMER || null
};
FileProgress.prototype.reset = function() {
    this.fileProgressElement.className = "progressContainer";
    this.fileProgressElement.childNodes[2].innerHTML = "&nbsp;";
    this.fileProgressElement.childNodes[2].className = "progressBarStatus";
    this.fileProgressElement.childNodes[3].className = "progressBarInProgress";
    this.fileProgressElement.childNodes[3].style.width = "0%";
    this.appear()
};
FileProgress.prototype.setProgress = function(a) {
    this.fileProgressElement.className = "progressContainer green";
    this.fileProgressElement.childNodes[3].className = "progressBarInProgress";
    this.fileProgressElement.childNodes[3].style.width = a + "%";
    this.appear()
};
FileProgress.prototype.setComplete = function() {
    this.fileProgressElement.className = "progressContainer blue";
    this.fileProgressElement.childNodes[3].className = "progressBarComplete";
    this.fileProgressElement.childNodes[3].style.width = "";
    var a = this;
    this.setTimer(setTimeout(function() {
        a.disappear()
    },
    3000))
};
FileProgress.prototype.setError = function() {
    this.fileProgressElement.className = "progressContainer red";
    this.fileProgressElement.childNodes[3].className = "progressBarError";
    this.fileProgressElement.childNodes[3].style.width = "";
    var a = this;
    this.setTimer(setTimeout(function() {
        a.disappear()
    },
    3000))
};
FileProgress.prototype.setCancelled = function() {
    this.fileProgressElement.className = "progressContainer";
    this.fileProgressElement.childNodes[3].className = "progressBarError";
    this.fileProgressElement.childNodes[3].style.width = "";
    var a = this;
    this.setTimer(setTimeout(function() {
        a.disappear()
    },
    2000))
};
FileProgress.prototype.setStatus = function(a) {
    this.fileProgressElement.childNodes[2].innerHTML = a
};
FileProgress.prototype.toggleCancel = function(b, c) {
    this.fileProgressElement.childNodes[0].style.visibility = b ? "visible": "hidden";
    if (c) {
        var a = this.fileProgressID;
        this.fileProgressElement.childNodes[0].onclick = function() {
            c.cancelUpload(a);
            return false
        }

    }

};
FileProgress.prototype.appear = function() {
    if (this.getTimer() !== null) {
        clearTimeout(this.getTimer());
        this.setTimer(null)
    }
    if (this.fileProgressWrapper.filters) {
        try {
            this.fileProgressWrapper.filters.item("DXImageTransform.Microsoft.Alpha").opacity = 100
        }
        catch(a) {
            this.fileProgressWrapper.style.filter = "progid:DXImageTransform.Microsoft.Alpha(opacity=100)"
        }

    }
    else {
        this.fileProgressWrapper.style.opacity = 1
    }
    this.fileProgressWrapper.style.height = "";
    this.height = this.fileProgressWrapper.offsetHeight;
    this.opacity = 100;
    this.fileProgressWrapper.style.display = ""
};
FileProgress.prototype.disappear = function() {
    var f = 15;
    var c = 4;
    var b = 30;
    if (this.opacity > 0) {
        this.opacity -= f;
        if (this.opacity < 0) {
            this.opacity = 0
        }
        if (this.fileProgressWrapper.filters) {
            try {
                this.fileProgressWrapper.filters.item("DXImageTransform.Microsoft.Alpha").opacity = this.opacity
            }
            catch(d) {
                this.fileProgressWrapper.style.filter = "progid:DXImageTransform.Microsoft.Alpha(opacity=" + this.opacity + ")"
            }

        }
        else {
            this.fileProgressWrapper.style.opacity = this.opacity / 100
        }

    }
    if (this.height > 0) {
        this.height -= c;
        if (this.height < 0) {
            this.height = 0
        }
        this.fileProgressWrapper.style.height = this.height + "px"
    }
    if (this.height > 0 || this.opacity > 0) {
        var a = this;
        this.setTimer(setTimeout(function() {
            a.disappear()
        },
        b))
    }
    else {
        this.fileProgressWrapper.style.display = "none";
        this.setTimer(null)
    }

};
