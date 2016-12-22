// JavaScript Document
$(document).ready(function(){
	//TODO validation when click
	$("#title").validation({notnull:"true"});
	$("#charge_price").validation({require:"money",checkByte:false});
	
$('.tag_check').click(function(){
    var id=$(this).attr('id').substr(9);
    $('.build_adv').hide();
    $('.build_adv').eq(id).show();
});
$('#alltype, #selftype').click(function(){
    if($(this).val() === 'all' ){
        mask();
		$('#mbox').show();
    }
    else{ 
		 $('#mbox').hide();
        $('#mask').remove();       
    }
});
$('#showeverywhere').click(function(){
	mask_area('area_more_sets');
	$('.notice_info').hide();
});
$('#showspecial').click(function(){
	unmask_area('area_more_sets');
	$('.notice_info').show();
});

$('span.cls').click(function(){
	$('#mask').remove();
	$('#mbox').hide();
})

$("#next_step").click(function(){
	var ad_plan_id = "<!--{$current_plan_id}-->";
		if (! ad_plan_id) {
		ad_plan_id    = $("#ad_plan_id").val();
	}
  var title         = $("#title").val();
  var type          = $('.tag_check').filter("input:checked").val();
  var charge_type   = $("#charge_type").val();
  var charge_price  = $("#charge_price").val();
  var is_priority   = $("#is_priority").attr('checked');
  var priority      = $("#priority").val();
  var is_ad_limit   = $("#is_ad_limit").attr('checked');
  var ad_limit_type = $("#ad_limit_type").val();
  var ad_limit      = $("#ad_limit").val();
  var is_uv_limit   = $("#is_uv_limit").attr('checked');
  var uv_limit_type = $("#uv_limit_type").val();
  var uv_limit      = $("#uv_limit").val();
    $.ajax({
      type: "POST",
      url: "/adgroup/new",
      dataType: "json",
      data: "ad_plan_id=" + ad_plan_id + 
            "&title=" + title + 
            "&type=" + type + 
            "&charge_type=" + charge_type + 
            "&charge_price=" + charge_price +
            "&is_priority=" + is_priority +
            "&priority=" + priority +
            "&is_ad_limit=" + is_ad_limit +
            "&ad_limit_type=" + ad_limit_type +
            "&ad_limit=" + ad_limit +
            "&is_uv_limit=" + is_uv_limit +
            "&uv_limit_type=" + uv_limit_type +
            "&uv_limit=" + uv_limit,
      success: function(msg){
          if (msg.status){
            alert('添加成功');
            window.location.href="/ad/new/?plan_id=" + ad_plan_id + "&group_id=" + msg.message;
            //TODO redirect to add ad group page
          } else {
            alert('添加失败');
            //TODO process where add failed(1 sql validate,2 db error)
          }
      }
   });
});

$("#previous_step").click(function(){
  window.location.href="/adplan/new";
});

$("#cancel_add").click(function(){
    window.location.href="/adplan/list";
});

});
function mask(){
	var m = "mask";    
	//mask遮罩层
	var newMask = document.createElement("div");
	newMask.id = m;
	newMask.style.position = "absolute";
	newMask.style.zIndex = "1";
	scrollWidth = Math.max(document.body.scrollWidth,document.documentElement.scrollWidth);
	scrollHeight = Math.max(document.documentElement.clientHeight,document.documentElement.scrollHeight);
	newMask.style.width = scrollWidth + "px";
	newMask.style.height = scrollHeight+ "px";
	newMask.style.top = "0px";
	newMask.style.left = "0px";
	newMask.style.background = "#33393C";
	newMask.style.filter = "alpha(opacity=50)";
	newMask.style.opacity = "0.50";
	document.body.appendChild(newMask);
	}
function mask_area(c){
	var _container = $i(c);
	if($i(_container.id + "_mask")) return;
	_container.style.position = "relative";
	var _mask = document.createElement('div');
	_mask.id = _container.id + "_mask";
	_mask.style.cssText = "position:absolute;top:0;left:0;width:" + _container.offsetWidth + "px;height:" + (_container.offsetHeight-10) + "px;background-color:#F7FAFF;border:1px solid #E8F1FF;text-align:center;font-size:32px;font-family:'黑体';";
	_mask.innerHTML = '<p style="margin-top:60px;">选择所有分类</p>';
	_container.appendChild(_mask);
}

function unmask_area(c){
	var _container = $i(c);
	//try {
		_container.removeChild($i(_container.id + "_mask"));
	//} catch(e) {}
}


// 增加类别
function add_area(){
	var _areas = $i('areas').getElementsByTagName('li');
	var _selected;
	for (var i=0;i<_areas.length;i++) {
		if (_areas[i].className.indexOf('selected') > -1) {_selected = _areas[i];}
	}

	try{
		_selected.getAttribute('areaid');
	}catch(e){
		return;
	}

	var _choosed = $i('choosed_areas').getElementsByTagName('li');
	if (_choosed.length >= 20) {
		alert('您最多可选择20个分类');
		return;
	}
	var minused = [];
	for (var i=0;i<_choosed.length;i++) {
		if (_choosed[i].getAttribute('areaid') == _selected.getAttribute('areaid')) return;
		if (_choosed[i].getAttribute('parentareacode') == _selected.getAttribute('areaid')) {
			minused.push(_choosed[i].getAttribute('areaid'));
		}
		if (_selected.getAttribute('areaid') == '9999' && _choosed[i].getAttribute('areaid') != '574' && _choosed[i].getAttribute('areaid') != '599' && _choosed[i].getAttribute('areaid') != '576' && _choosed[i].getAttribute('areaid') != '577') {
			minused.push(_choosed[i].getAttribute('areaid'));
		}
	}

	for (var i=0;i<_choosed.length;i++) {
		for (var j=0;j<minused.length;j++) {
			if (_choosed[i].getAttribute('areaid') == minused[j]) {
				try{
					$i('choosed_areas').removeChild(_choosed[i]);
				}catch(e){}
			}
		}
	}

	append_area(_selected.innerHTML, _selected.getAttribute('areaid'), _selected.getAttribute('parentareacode'));
	gray_area_sources(_selected);
}

function update_selected () {
	var _lis = $i('choosed_areas').getElementsByTagName('li'), selected = '', selected_name = '';
	for (var i=0;i<_lis.length;i++) {
		selected = selected == '' ? _lis[i].getAttribute('areaid') : selected + ',' + _lis[i].getAttribute('areaid');
		var parn=selected.substr(0,1);
		selected_name = selected_name == '' ? _lis[i].innerHTML : selected_name + ', ' + _lis[i].innerHTML;
	}
	if($i("showeverywhere").checked)
	{ selected_name = 'all';selected=0;}
	alert(selected);
	//alert(selected_name+'\n'+selected);
	//window.location.href="?type="+selected_name;
	//$w.$l.href="?type="+selected_name;
			}

function append_area(an, ac, pc){
	var _new = document.createElement('li');
	_new.setAttribute('areaid', ac);
	_new.setAttribute('parentareacode', pc);
	_new.innerHTML = an;
	_new.onclick = function(e) {
		var ev = !e ? window.event : e;
		var _li = _jsc.evt.gTar(ev);
		var __choosed = $i('choosed_areas').getElementsByTagName('li');
		for (var j=0;j<__choosed.length;j++) {
			__choosed[j].className = '';
		}
		_li.className = 'selected';
	};
	return $i('choosed_areas').appendChild(_new);
}

// 移除类别
function remove_area(){
	var _choosed = $i('choosed_areas').getElementsByTagName('li');
	var _selected;
	for (var i=0;i<_choosed.length;i++) {
		if (_choosed[i].className == 'selected') _selected = _choosed[i];
	}

	try{
		_selected.getAttribute('areaid');
	}catch(e){
		if(_choosed.length > 0){
			alert("请选择要移除的类型！");
		}	
		return;
	}

	ungray_area_sources(_selected);
	$i('choosed_areas').removeChild(_selected);
}

function gray_area_sources(_selected){
	var _areas = $i('areas').getElementsByTagName('li');
	if (_selected.getAttribute('parentareacode') == '0') {
		for (var i=0;i<_areas.length;i++) {
			if (_areas[i].getAttribute('parentareacode') == _selected.getAttribute('areaid') && _areas[i].className.indexOf('added') < 0) {_areas[i].className += ' added';}
			if (_selected.getAttribute('areaid') == '9999' && _areas[i].className.indexOf('added') < 0 && i > 4) {_areas[i].className += ' added';}
		}
	}
	if (_selected.className.indexOf('added') < 0) _selected.className += ' added';
}

function ungray_area_sources(_selected){
	var _areas = $i('areas').getElementsByTagName('li');
	for (var i=0;i<_areas.length;i++) {
		if (_selected.getAttribute('parentareacode') == '0') {
			if (_areas[i].getAttribute('parentareacode') == _selected.getAttribute('areaid')) {
				_areas[i].className = _areas[i].className.replace('added', '');
			}
		}
		if (_selected.getAttribute('areaid') == '9999' && i > 4) {_areas[i].className = _areas[i].className.replace('added', '');}
		if (_areas[i].getAttribute('areaid') == _selected.getAttribute('areaid')) {
			_areas[i].className = _areas[i].className.replace('added', '');
			if (_selected.getAttribute('parentareacode') != '0') break;
		}
	}
}

// 初始化地域数据到控件
function init_areas(){
	var _roots = {};
	for (var i=0;i<addr_data.length;i++) {
		try{
			if (addr_data[i].parentareacode == '0') {
				eval("_roots.a_" + addr_data[i].areacode + "={\"areaname\":\"" + addr_data[i].areaname + "\", \"arealevel\":\"" + addr_data[i].arealevel + "\", \"areacode\":\"" + addr_data[i].areacode + "\", \"parentareacode\":\"" + addr_data[i].parentareacode + "\"}");
				eval("_roots.a_" + addr_data[i].areacode + ".subs=[]");
			} else {
				eval("_roots.a_" + addr_data[i].parentareacode + ".subs.push({\"areaname\":\"" + addr_data[i].areaname + "\", \"arealevel\":\"" + addr_data[i].arealevel + "\", \"areacode\":\"" + addr_data[i].areacode + "\", \"parentareacode\":\"" + addr_data[i].parentareacode + "\"})");
			}
		}catch(e){
			//alert(typeof addr_data);
			break;
		}
	}

	for (area in _roots) {
		//add_log(_roots[area].areaname);
		new_area(_roots[area].areacode, _roots[area].areaname, _roots[area].arealevel, _roots[area].parentareacode, false);
		if (_roots[area].subs[0]) {
			for (var i=0;i<_roots[area].subs.length;i++) {
				//add_log('&nbsp;&nbsp;' + _roots[area].subs[i].areaname);
				new_area(_roots[area].subs[i].areacode.split('|')[1], _roots[area].subs[i].areaname, _roots[area].subs[i].arealevel, _roots[area].subs[i].parentareacode, true);
			}
		}
	}
}

// 在type下新增项
function new_area(ac, an, al, pac, issub){
	var _li = document.createElement('li');
	_li.id = "area_" + ac;
	_li.innerHTML = an;
	_li.setAttribute('areaid', ac);
	_li.setAttribute('arealevel', al);
	_li.setAttribute('parentareacode', pac);
	if (issub) _li.className = 'issub';
	_li.onclick = function(e) {
		var ev = !e ? window.event : e;
		var _li = _jsc.evt.gTar(ev);
		if (_li.className.indexOf('added') > -1) return;
		var __areas = $i('areas').getElementsByTagName('li');
		var isparentid = _li.getAttribute("arealevel") == "0" ? true :false;
		
		for (var j=0;j<__areas.length;j++) {
			__areas[j].className = __areas[j].className.replace('selected', '');
			if(isparentid){
				__areas[j].className = __areas[j].className.replace('show', '');
				if(_li.getAttribute("areaid") == __areas[j].getAttribute("parentareacode"))
					__areas[j].className += " show";
			}
		}
		_li.className += ' selected';
	};
	$i('areas').appendChild(_li);
}

init_areas();

if ($i('showeverywhere').checked) mask_area('area_more_sets');
	_jsc.util.addEvent(window,"load",function(){
		
});
function modifyCpcTransAreas(){
	//alert($i('choosed_areas_name_flag').value);
	$i('areasform').submit();
}		