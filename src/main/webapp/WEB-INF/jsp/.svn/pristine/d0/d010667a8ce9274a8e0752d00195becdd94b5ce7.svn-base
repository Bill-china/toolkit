// JavaScript Document
$(document).ready(function(){
    $(".ad-tab tr:odd").addClass("odd");
    $(".ad-tab tr").hover(function(){
        $(this).addClass("bg_hover");
        },function(){
            $(this).removeClass("bg_hover");
        })
    autoClose(3000, "notice");
    autoClose(3000, "money_notice");
/*    $('.feedback').click(function(){
        new Boxy('<iframe src="/help/feedback/" height="320px" width="480px" scrolling="no"frameborder="no" border="0" marginwidth="0" marginheight="0" allowtransparency="yes"></iframe>',{title:"给我们留言",modal:true,closeable : true});
    })*/
    $("#favorite").click(function() {
        var ctrl = (navigator.userAgent.toLowerCase()).indexOf('mac') != -1 ? 'Command/Cmd': 'CTRL';
        if (document.all) {
            window.external.addFavorite('http://vip.ad.360.cn', '360大客户系统')
        } else if (window.sidebar) {
            window.sidebar.addPanel('360大客户系统', 'http://vip.ad.360.cn', "")
        } else {
            alert('您可以尝试通过快捷键' + ctrl + ' + D 加入到收藏夹~')
        }
    })
    $("#mid-btn").click(function(){
        if($("#left").css("display")=="none"){
            $("#left").css("display","inline");
            $("#right").css("margin-left","221px");
            $(this).removeClass("cur");
            if(!!window.ActiveXObject && !window.XMLHttpRequest){
                $("#right").css("margin-left","0");
                }
        }else{
            $("#left").css("display","none");
            $("#right").css("margin-left","0");
            $(this).addClass("cur");
        };
    });
    $(".hopen").click(function(){
        if($(".help ul").css("display")=="none"){
            $(".help ul").css("display","block");
            $(".helpa1").css("display","block");
            $(".helpa").css("display","none");
            $(this).removeClass("cur");
        }else{
            $(".help ul").css("display","none");
            $(".helpa1").css("display","none");
            $(".helpa").css("display","block");
            $(this).addClass("cur");
        };
    });

    $(".ocdoor").doors();
    $(".ocdoor1").doors();
    $(".ocdoor2").doors();
	$(".ad-tree li a").click(function(){
    $(".ad-tree li a").removeClass("cur");
    $(this).addClass("cur");
});
})
$.fn.doors=function(){
    var _this=this;
    _this.click(function(e){
    var span=$(e.target);
    var div =span.parents(".ad-tree").find("ul");
    var div1 =span.parents(".form-list").find(".form-c");
    span.toggleClass("open");
    div.toggle();
    div1.toggle();
    })
}
function dateDiff(d1,d2){
    var result = Date.parse(d1.replace(/-/g,"/"))- Date.parse(d2.replace(/-/g,"/"));
    return result;
}
//滚动到顶部
var scope = scope|| {};
scope.ua=navigator.userAgent;
scope._ua=scope.ua.toLowerCase();
scope.$IE6=/msie 6/.test(scope._ua);
var $IE=scope.$IE,$MOZ=scope.$MOZ,$IE6=scope.$IE6,$IOS=scope.$IOS,$HTML5 = {};
var scrolltotop={
    setting: {startline:1, scrollto: 0, scrollduration:600, fadeduration:[500, 100]},
    controlHTML: '<a href="#top">↑TOP</a>',
    controlattrs: {
        offsetx:0,
        offsety:77},
    anchorkeyword: '#top', 
    state: {isvisible:false, shouldvisible:false},
    scrollup:function(){
        if (!this.cssfixedsupport)
            this.$control.css({opacity:0})
        var dest=isNaN(this.setting.scrollto)? this.setting.scrollto : parseInt(this.setting.scrollto)
        if (typeof dest=="string" && jQuery('#'+dest).length==1) 
            dest=jQuery('#'+dest).offset().top
        else
            dest=0
        this.$body.animate({scrollTop: dest}, this.setting.scrollduration);
    },
    keepfixed:function(){
        var $window=jQuery(window)
        var controlx=$window.scrollLeft() + $window.width() - this.$control.width() - this.controlattrs.offsetx
        var controly=$window.scrollTop() + $window.height() - this.$control.height() - this.controlattrs.offsety
        this.$control.css({left:controlx+'px', top:controly+'px'})
    },
    togglecontrol:function(){
        var scrolltop=jQuery(window).scrollTop()
        if (!this.cssfixedsupport)
            this.keepfixed()
        this.state.shouldvisible=(scrolltop>=this.setting.startline)? true : false
        if (this.state.shouldvisible && !this.state.isvisible){
            this.$control.stop().animate({opacity:1}, this.setting.fadeduration[0])
            this.state.isvisible=true
        }
        else if (this.state.shouldvisible==false && this.state.isvisible){
            this.$control.stop().animate({opacity:0}, this.setting.fadeduration[1])
            this.state.isvisible=false
        }
    },
    init:function(){
        jQuery(document).ready(function($){
            var mainobj=scrolltotop
            var iebrws=document.all
            mainobj.cssfixedsupport=!iebrws || iebrws && document.compatMode=="CSS1Compat" && window.XMLHttpRequest 
            mainobj.$body=(window.opera)? (document.compatMode=="CSS1Compat"? $('html') : $('body')) : $('html,body')
            mainobj.$control=$('<div id="topcontrol">'+mainobj.controlHTML+'</div>')
                .css({position:mainobj.cssfixedsupport? 'fixed' : 'absolute', bottom:mainobj.controlattrs.offsety, right:mainobj.controlattrs.offsetx, opacity:0, cursor:'pointer'})
                .attr({title:'↑TOP'})
                .click(function(){mainobj.scrollup(); return false})
                .appendTo('body')
            if (document.all && !window.XMLHttpRequest && mainobj.$control.text()!='') 
                mainobj.$control.css({width:mainobj.$control.width()}) 
            mainobj.togglecontrol()
            $('a[href="' + mainobj.anchorkeyword +'"]').click(function(){
                mainobj.scrollup()
                return false
            })
            $(window).bind('scroll resize', function(e){
                mainobj.togglecontrol()
                refreshPos();
            });

            function refreshPos(){
                mainobj.$control.css({"right":($(window).width()-1060)/2-19+"px"});
                if($IE6){
                    scrolltotop.controlattrs.offsetx=$("div.w960").offset().left-60
                    }
                }
        })
    }
};
function autoClose(n, obj) {
    if($('.'+obj).length > 0){
        setTimeout(function(){
            $('.'+obj).slideUp(500);
        }, n);
    }else{
        return;
    }
}

function mask() {
    var m = "mask";
    //mask遮罩层
    var newMask = document.createElement("div");
    newMask.id = m;
    newMask.style.position = "absolute";
    newMask.style.zIndex = "1";
    scrollWidth = Math.max(document.body.scrollWidth, document.documentElement.scrollWidth);
    scrollHeight = Math.max(document.documentElement.clientHeight, document.documentElement.scrollHeight);
    newMask.style.width = scrollWidth + "px";
    newMask.style.height = scrollHeight + "px";
    newMask.style.top = "0px";
    newMask.style.left = "0px";
    newMask.style.background = "#33393C";
    newMask.style.filter = "alpha(opacity=50)";
    newMask.style.opacity = "0.50";
    document.body.appendChild(newMask);
}
function popUp(){
    var $doc = $(document),
        $popUp = $(".pop-up"),
        $newChannel = $(".new-channel"),
        $warpLayer = $(".warp-layer"),
        $clsBtn = $("#close-btn"),
        popUpW = $popUp.outerWidth(true),
        popUpH = $popUp.outerHeight(true),
        newChW = $newChannel.outerWidth(true),
        newChH = $newChannel.outerHeight(true);
    $(".ad-tab table tr").find("td").each(function(idx) {
        $(".td_bg"+idx).live("click",function(){
            var docW =$doc.width(),
                docH = $doc.height();
            $warpLayer.css({"width":docW,"height":docH}).show();
            $popUp.css({"left":"50%","top":"50%","margin-left":-popUpW/2,"margin-top":-popUpH/2}).hide().show();
            $(".shadow").show();
        });
    });	
    $(".form-list dl dd").find("a").each(function(i) {
        i+=1;
        $("#channel"+i).live("click",function(){
            var docW =$doc.width(),
                docH = $doc.height();
            $(".shadow").show();
            $warpLayer.css({"width":docW,"height":docH}).show();
            $newChannel.find("table").hide().end().find("#new-channel"+i).show();
            $newChannel.css({"left":"50%","top":"50%","margin-left":-newChW/2,"margin-top":-newChH/2}).hide().show();	
        });
    });
    $clsBtn.live("click",function(){
        $(".shadow").hide();
        $warpLayer.hide();
        $popUp.hide();
        $newChannel.hide();
    });
};