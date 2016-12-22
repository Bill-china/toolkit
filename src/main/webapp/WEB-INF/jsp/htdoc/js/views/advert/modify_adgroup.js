$(document).ready(function() {
    //根据选择值，输出相关数据
    var have_data = $('#word_areas').val(),
    have_data_arr,
    have_data_len,
    data_len_one;
    if (have_data) {
        have_data_arr = have_data.split('\n');
        have_data_len = have_data_arr.length;
        data_len_one = have_data_arr[0];
    } else {
        have_data_len = 0;
        data_len_one = "";
    }
    if (channelid == 9) {
        if (placeid == 92) {
            atrprice = 0.2;
        } else {
            atrprice = 0.1;
        }
    }
    if (channelid == 11 || channelid == 12) {
        atrprice = 0.1;
    }
    if (channel_default == '搜索推广') {
        atrprice = 0.3;
    }
    if (channelid == 8) {
        for (i = 0; i < price.length; i++) {
            if (placeid == price[i].sid) {
                atrprice = price[i].val;
            }
        }
    }
    $('#bid').attr('min', atrprice);
    $('#advise_price').html(atrprice);
    $('#have_choose').html(data_len_one);
    $('#keyword_number').html(have_data_len);
    $('#choose_word_number').html(have_data_len);
    var choose_word_number = parseInt($('#choose_word_number').html());
    if (area_get_date[0].areacode != 0) {
        $('#arear_info').html('仅在选定地域投放');
    }
    if (signtype) {
        if (insterest_get_date.length > 0 && insterest_get_date[0].areacode != 9999) {
            $('#interest_info').html('仅在选定兴趣投放');
        }
    }

/*------------------------------------------关键词修改操作开始-------------------------------------------*/
    //选择关键词
    $('#add_keyword').click(function() {
        var word_areas_len = $('#word_areas').val().split('\n').length;
        choose_word_number = word_areas_len;
        $('#choose_word_number').html(choose_word_number);
        var channel = $('#channel_name').html();
        if (channel === '猜你喜欢') {
            getdata_url = '/adgroup/ajaxgetkeywordcainiinfo/';
            keywordlen = 50;
            $('#totaln').html('50');
        } else {
            getdata_url = '/adgroup/ajaxgetkeywordinfo/';
            keywordlen = 100;
            $('#totaln').html('100');
        }
        mask();
        $('#keyword_box').show();
    })

    //键盘回车和删除关联关键词操作事件
    $('#word_areas').live('keyup keydown', function(event) {
        if (choose_word_number >= keywordlen && event.keyCode == 13) {
            return false;
        } else {
            if (!$.trim($(this).val())) {
                choose_word_number = 0;
                $('#choose_word_number').html(0);
            } else {
                var chose_length = $(this).val().split('\n').length;
                choose_word_number = chose_length;
                $('#choose_word_number').html(choose_word_number);
            }
        }
    })
    
    
    //搜索框获取焦点触发清楚提示语句事件
    $('#search_box').focus(function() {
        if ($(this).val() == '请输入一个关键词，我们会为您搜索相关的关键词') {
            $(this).val('');
        }
    })
    $('#search_box').live('keyup', function(event) {
        if (event.keyCode == 13) {
            $('#search_button').click();
        }
    })
    //搜索框获取焦点触发清楚提示语句事件
    $('#search_box').keydown(function() {
        $(this).addClass('black');
    })
    //点击搜索后获取数据
    $('#search_button').live('click', function() {
        var search_word = $('#search_box').val();
        $('#choosed_word').html('');
        if (!search_word) {
            $('#search_box').removeClass('black').val('请输入一个关键词，我们会为您搜索相关的关键词');
            return false;
        }
        if (search_word == '请输入一个关键词，我们会为您搜索相关的关键词') {
            return false;
        } else {
            $('#search_box').addClass('black');
            //choose_word_number = 0;
            $('#no_result').hide();
            $('#have_result').hide();
            $('#load_data').show();
            //$('#word_areas').val('');
            //$('#choose_word_number').html('0');
            $('#choosed_word tr').removeClass('gray');
            var datahtml = '';
            $.getJSON(getdata_url, {
                'search_word': search_word
            },
            function(data) {
                if (!data) {
                    $('#choosed_word').html('<tr><td colspan="3" align="center">无查询结果，请搜索其它关键词</td></tr>');
                } else {
                    $.each(data.kr_res, function(i, item) {
                        datahtml += '<tr><td class="result_word">' + item.word + '</td><td>' + item.pv + '</td><td><span class="sche"><em style="width:' + item.compete + '%;">' + item.compete + '</em></span></td></tr>';
                    })
                }
                $('#choosed_word').html(datahtml);
                $('#load_data').hide();
                $('#have_result').show();
            })
        }
    })
    //排序事件
    $('th:has(".sort")').click(function() {
        var this_sort = $(this).children('.sort');
        $('.sort').not(this_sort).attr('class', "sort");
        if (this_sort.hasClass('sort-up')) {
            this_sort.removeClass('sort-up').addClass('sort-down');
        } else if (this_sort.hasClass('sort-down')) {
            this_sort.removeClass('sort-down').addClass('sort-up');
        } else {
            this_sort.addClass('sort-up');
        }
        $("table#list_tab").tablesorter();
    })
    //添加关键词操作事件
    $('#choosed_word tr').live('click', function() {
        var rechose = true,
        _this = $(this),
        _thishtml = _this.children(":first").html(),
        areas_val = $('#word_areas').val(),
        areas_array = areas_val.split('\n');
        if (_this.hasClass('gray') == true) {
            return false;
        }
        if (choose_word_number > (keywordlen - 1)) {
            alert('您的选择数量已达到最大限值！');
            return false;
        } else {
            _this.addClass('gray');
            $.each(areas_array, function(i) {
                if (areas_array[i] == _thishtml) {
                    rechose = false;
                    return false;
                }
            })
            if (rechose) {
                $('#word_areas').val(areas_val + _thishtml + '\n');
                choose_word_number += 1;
            }
            $('#choose_word_number').html(choose_word_number);
        }
    })
    //添加全部
    $('#word_all_add').click(function() {
        var result_number = $('#choosed_word tr').length;
        var all_number = result_number > keywordlen ? keywordlen - choose_word_number: result_number - choose_word_number;
        //console.log($('#choosed_word tr').not('[class=gray]').slice(0,10));
        $('#choosed_word tr').not('[class=gray]').slice(0, all_number).click();
    })
    //点击保存关键词操作事件
    $('#save_choose_word').click(function() {
        if (choose_word_number > keywordlen) {
            alert('您选择的关键词数量超出最大限值！');
            return false;
        }
        var chose_first = $('#word_areas').val().split('\n')[0];
        $('#form_word').remove();
        $('#modify').append($('<input type="hidden" name="keywordarr" id="form_word"/>').val($('#word_areas').val()));
        $('#have_choose').html(chose_first);
        $('#keyword_number').html(choose_word_number);
        $('#mask').remove();
        $('#keyword_box').hide();
    })
    //关闭关键词选择框
    $('.close_box').click(function() {
        $('#mask').remove();
        $('#keyword_box').hide();
    })

/*------------------------------------------关键词修改操作结束-------------------------------------------*/



/*------------------------------------------否定关键词修改操作开始-------------------------------------------*/
    //添加否定词tab切换
    $('.tab_tagli').click(function(){
        tagid = $(this).attr('tagid');
        $('.tab_tagli').removeClass('cur');
        $(this).addClass("cur");
        $('.choosed_number').hide().eq(tagid).show();
        $('.deny_word_area').hide().eq(tagid).show();
        if(tagid == 0){
            $('#contrast').html('完全包含');
            domcn = $('#ncwn');
        }else{
            $('#contrast').html('完全一致');
            domcn = $('#ncwne');
        }
        ncwn = domcn.html();
    })
    
    //键盘回车和删除关联否定关键词操作事件 2012-09-13
    $('.deny_word_area').live('keyup keydown', function(event) {
        if (ncwn >= keywordlen && event.keyCode == 13) {
            return false;
        }else{
            if(!$.trim($(this).val())){
                ncwn = 0;
                domcn.html(0);
            }else {
                var chose_length = $(this).val().split('\n').length;
                ncwn = chose_length;
                domcn.html(ncwn);
            }
        }
    })

    //否定关键词输入框获取焦点触发清楚提示语句事件
    $('.deny_word_area').focus(function() {
        console.log($(this).val());
        if($(this).val() == '请输入否定关键词') {
            $(this).val('');
        }
    })
    
    
        //添加否定关键词的保存操作事件
    $('#save_negative_choose_word').click(function() {
        var number_ncwn = parseInt($('#ncwn').html()),number_ncwne = parseInt($('#ncwne').html());
        if (number_ncwn > keywordlen || number_ncwne > keywordlen){
            alert('您选择的否定关键词数量超出最大限值！');
            return false;
        }
        var negative_word_end = $('#negative_word_areas').val();
        var negative_word_contain_end = $('#negative_word_areas_contain').val();
        if ( (!$.trim(negative_word_end) || (negative_word_end == '请输入否定关键词')) && (!$.trim(negative_word_contain_end) || (negative_word_contain_end == '请输入否定关键词')) ) {
            alert('无否定关键词');
            return false;
        }
        var chose_first = negative_word_end.split('\n')[0];
        $('#negative_form_word').val(negative_word_end);
        $('#negative_form_word_contain').val(negative_word_contain_end);
        $('#negative_keyword_number').html(number_ncwn);
        $('#negative_keyword_number_cont').html(number_ncwne);
        $('#mask').remove();
        $('#negative_keyword_box').hide();
    })
    
    //点击弹窗的关闭按钮事件
    $('.negative_close_box').click(function() {
        $('#mask').remove();
        $('#negative_keyword_box').hide();
        $('#negative_keyword_number').html(number_ncwn+number_ncwne);
    })
    
    //TODO validation when click
    
    //添加否定关键词入口，点击显示否定关键词输入框  xiaoang 2012-12-25
    $('#negative_add_keyword').click(function() {
        var _this_val="";
            if($('.tab_tagli').eq(0).hasClass('cur')){
                tagid = 0;
                domcn = $('#ncwn');
                _this_val = $('.deny_word_area').eq(0).val();
            }else{
                tagid = 1;
                domcn = $('#ncwne');
                _this_val = $('.deny_word_area').eq(1).val();
            }
            if(!$.trim($('.deny_word_area').eq(tagid).val()) || $('.deny_word_area').eq(tagid).val() == '请输入否定关键词'){
                ncwn = 0;
                domcn.html(0);
            }else{
                var chose_length = _this_val.split('\n').length;
                ncwn = chose_length;
                domcn.html(ncwn);
            }
        mask();
        $('#negative_keyword_box').show();
    })

/*------------------------------------------否定关键词修改操作结束-------------------------------------------*/



    $('#choosed_areas li').live('click', function() {
        $('#choosed_areas li').removeClass('selected');
        $(this).addClass('selected');
    })
    //选择自定义投放地域事件
    $('#selftype').click(function() {
        if (choose_type == 1) {
            choose_type = 0;
        }
        $('#mbox_title').html('选择投放区域');
        $('#areas').empty();
        init_areas();
        $('#choosed_areas').empty();
        if (area_choose_bolle == true) {
            if (area_get_date.length == 1 && area_get_date[0].areacode == 0) {
                $('#choosed_areas').html('<li areaid="0" parentareacode="0">全部</li>');
                $('#areas li').addClass('added');
            } else {
                $.each(area_get_date, function(i, item) {
                    $('#choosed_areas').append('<li areaid=' + item.areacode + ' parentareacode=' + item.parentareacode + '>' + item.areaname + '</li>');
                    $('#areas li[areaid="' + item.areacode + '"]').addClass('added');
                });
            }
        } else {
            if (area_arr.length == 1 && area_arr[0].areacode == 0) {
                $('#choosed_areas').html('<li areaid="0" parentareacode="0">全部</li>');
                $('#areas li').addClass('added');
            } else {
                $.each(area_arr, function(i, item) {
                    $('#choosed_areas').append('<li areaid=' + item.areacode + ' parentareacode=' + item.parentareacode + '>' + item.areaname + '</li>');
                    $('#areas li[areaid="' + item.areacode + '"]').addClass('added');
                });
            }
        }
        mask();
        $('#mbox').show();
    })

    //选择自定义兴趣事件
    $('#selfinterest').click(function() {
        if (choose_type == 0) {
            choose_type = 1;
        }
        $('#mbox_title').html('选择兴趣');
        $('#areas').empty();
        init_interest();
        $('#choosed_areas').empty();
        if (interest_choose_bolle == true) {
            if (insterest_get_date.length == 1 && insterest_get_date[0].areacode == 9999) {
                $('#choosed_areas').html('<li areaid="9999" parentareacode="0">全部</li>');
                $('#areas li').addClass('added');
            } else {
                $.each(insterest_get_date, function(i, item) {
                    $('#choosed_areas').append('<li areaid=' + item.areacode + ' parentareacode=' + item.parentareacode + '>' + item.areaname + '</li>');
                    $('#areas li[areaid=' + item.areacode + ']').addClass('added');
                });
            }
        } else {
            if (interest_arr.length == 1 && interest_arr[0].areacode == 9999) {
                $('#choosed_areas').html('<li areaid="9999" parentareacode="0">全部</li>');
                $('#areas li').addClass('added');
            } else {
                $.each(interest_arr, function(i, item) {
                    $('#choosed_areas').append('<li areaid=' + item.areacode + ' parentareacode=' + item.parentareacode + '>' + item.areaname + '</li>');
                    $('#areas li[areaid="' + item.areacode + '"]').addClass('added');
                });
            }
        }
        //interest_choose_bolle = false;
        mask();
        $('#mbox').show();
    })
    //关闭投放类型的弹出层
    $('span.cls, #error_sure').click(function() {
        $('#mask').remove();
        $('#mbox').hide();
        $('#mbox_error').hide();
    });
    //form绑定校验事件
    $("#modify").validate();
    //提交修改事件
    $("#confirm").bind("click", function() {
        var title_val = $("#title").val(),
        ad_plan_id = $("#ad_plan_id").val(),
        ad_group_id = $("#ad_group_id").val();
        $("#modify").validate().form();
        if ($("#modify").validate().checkForm() == true) {
            $.post("/adgroup/ajaxcheckupdate", {
                "title": title_val,
                "plan_id": ad_plan_id,
                "group_id": ad_group_id
            },
            function(data) {
                if (data == 1) {
                    alert('该推广组已存在！');
                    return false;
                } else {
                    $("#modify").submit();
                }
            })
        }
    })
    $("#cancel").click(function() {
        window.location.href = "/adgroup/list";
    });
});

function add_area() {
    var _areas = $i('areas').getElementsByTagName('li');
    var _selected;
    for (var i = 0; i < _areas.length; i++) {
        if (_areas[i].className.indexOf('selected') > - 1) {
            _selected = _areas[i];
        }
    }

    try {
        _selected.getAttribute('areaid');
    } catch(e) {
        return;
    }

    var _choosed = $i('choosed_areas').getElementsByTagName('li');
    if (_choosed.length >= 20) {
        //alert('您最多可选择20个分类');
        $i('error_info').style.display = '';
        $i('error_info').innerHTML = "您最多可选择20个分类！"
        return;
    }
    $i('error_info').style.display = 'none';
    $i('error_info').innerHTML = ""
    var minused = [];
    for (var i = 0; i < _choosed.length; i++) {
        if (_choosed[i].getAttribute('areaid') == _selected.getAttribute('areaid')) return;
        if (_choosed[i].getAttribute('parentareacode') == _selected.getAttribute('areaid')) {
            minused.push(_choosed[i].getAttribute('areaid'));
        }
        if (_selected.getAttribute('areaid') == '0') {
            minused.push(_choosed[i].getAttribute('areaid'));
        }
    }

    for (var i = 0; i < _choosed.length; i++) {
        for (var j = 0; j < minused.length; j++) {
            if (_choosed[i].getAttribute('areaid') == minused[j]) {
                try {
                    $i('choosed_areas').removeChild(_choosed[i]);
                } catch(e) {}
            }
        }
    }

    append_area(_selected.innerHTML, _selected.getAttribute('areaid'), _selected.getAttribute('parentareacode'));
    gray_area_sources(_selected);
}

//选择完成后执行更新操作
function update_selected() {
    if (choose_type == 0 && area_choose_bolle == false) {
        area_arr = [];
    }
    if (choose_type == 1 && interest_choose_bolle == false) {
        interest_arr = [];
    }
    var _choosed = $i('choosed_areas'),
    _areas = $i('areas').getElementsByTagName('li'),
    _lis = _choosed.getElementsByTagName('li'),
    selected = '',
    selected_name = '',
    selected_pa_id = '';
    for (var i = 0; i < _lis.length; i++) {
        //selected = selected == '' ? _lis[i].getAttribute('areaid') : selected + ',' + _lis[i].getAttribute('areaid');
        var lipar = _lis[i].getAttribute('parentareacode'),
        areaid = _lis[i].getAttribute('areaid'),
        chose_name = _lis[i].innerHTML,
        lithis;
        var choose_obj = {
            "areaname": chose_name,
            "areacode": areaid,
            "parentareacode": lipar
        };
        //if(choose_type == 0 && area_choose_bolle == false){
        if (choose_type == 0) {
            area_arr.push(choose_obj);
        }
        if (choose_type == 1) {
            interest_arr.push(choose_obj);
        }
        /*if(lipar === '0'){
          lithis =_lis[i].innerHTML;
          }else{
          lithis = $('#areas li[areaid='+lipar+']').html();
          }*/
        //selected_name 使用名称传值
        //selected_name = selected_name == '' ? _lis[i].innerHTML : selected_name + ', ' + _lis[i].innerHTML;
        //selected_name = selected_name == '' ? "{"+lithis +":"+ _lis[i].innerHTML+"}" : selected_name +",{"+ lithis +":"+_lis[i].innerHTML+"}";
        selected_pa_id = selected_pa_id == '' ? "{" + lipar + ":" + areaid + "}": selected_pa_id + ",{" + lipar + ":" + areaid + "}";

    }
    //    if($i("showeverywhere").checked)    { selected_name = 'all';selected=0;}
    /*    if (! selected_name) {
    // alert('请选择分类');
    $i('error_info').style.display='';
    $i('error_info').innerHTML="请选择分类！"
    return false;
    }*/
    if (!selected_pa_id) {
        // alert('请选择分类');
        $i('error_info').style.display = '';
        $i('error_info').innerHTML = "请选择要添加的项！"
        return false;
    }
    $i('error_info').style.display = 'none';
    $i('error_info').innerHTML = ""
    if (choose_type == 0) {
        area_choose_bolle = false;
        $("#area_chose").val(selected_pa_id);
        if (!selected_pa_id || selected_pa_id == "{0:0}") {
            $('#arear_info').html('全部');
        } else {
            $('#arear_info').html('仅在选定地域投放');
        }
    } else {
        interest_choose_bolle = false;
        $("#insterest_chose").val(selected_pa_id);
        if (!selected_pa_id || selected_pa_id == "{0:9999}") {
            $('#interest_info').html('全部');
        } else {
            $('#interest_info').html('仅在选定兴趣投放');
        }
    }
    $('#mask').remove();
    $('#mbox').hide();
    //alert(arrayname);
    //alert(selected_name+'\n'+selected);
    //window.location.href="?type="+selected_name;
    //$w.$l.href="?type="+selected_name;
}

function append_area(an, ac, pc) {
    var _new = document.createElement('li');
    _new.setAttribute('areaid', ac);
    _new.setAttribute('parentareacode', pc);
    _new.innerHTML = an;
    _new.onclick = function(e) {
        var ev = ! e ? window.event: e;
        var _li = _jsc.evt.gTar(ev);
        var __choosed = $i('choosed_areas').getElementsByTagName('li');
        for (var j = 0; j < __choosed.length; j++) {
            __choosed[j].className = '';
        }
        _li.className = 'selected';
    }
    return $i('choosed_areas').appendChild(_new);
}

// 移除类别
function remove_area() {
    var _choosed = $i('choosed_areas').getElementsByTagName('li');
    var _selected;
    for (var i = 0; i < _choosed.length; i++) {
        if (_choosed[i].className == 'selected') _selected = _choosed[i];
    }

    try {
        _selected.getAttribute('areaid');
    } catch(e) {
        if (_choosed.length > 0) {
            $i('error_info').style.display = '';
            $i('error_info').innerHTML = "请选择要移除的项！"
            //alert("请选择要移除的类型！");
        }
        return;
    }
    $i('error_info').style.display = 'none';
    $i('error_info').innerHTML = ""
    ungray_area_sources(_selected);
    $i('choosed_areas').removeChild(_selected);
}

function gray_area_sources(_selected) {
    var _areas = $i('areas').getElementsByTagName('li');
    if (_selected.getAttribute('parentareacode') == '0') {
        for (var i = 0; i < _areas.length; i++) {
            if (_areas[i].getAttribute('parentareacode') == _selected.getAttribute('areaid') && _areas[i].className.indexOf('added') < 0) {
                _areas[i].className += ' added';
            }
            if (_selected.getAttribute('areaid') == '0' && _areas[i].className.indexOf('added') < 0 && i > 0) {
                _areas[i].className += ' added';
            }
        }
    }
    if (_selected.className.indexOf('added') < 0) _selected.className += ' added';
}

function ungray_area_sources(_selected) {
    var _areas = $i('areas').getElementsByTagName('li');
    for (var i = 0; i < _areas.length; i++) {
        if (_selected.getAttribute('parentareacode') == '0') {
            if (_areas[i].getAttribute('parentareacode') == _selected.getAttribute('areaid')) {
                _areas[i].className = _areas[i].className.replace('added', '');
            }
        }
        if (_selected.getAttribute('areaid') == '0' && i > 0) {
            _areas[i].className = _areas[i].className.replace('added', '');
        }
        if (_areas[i].getAttribute('areaid') == _selected.getAttribute('areaid')) {
            _areas[i].className = _areas[i].className.replace('added', '');
            if (_selected.getAttribute('parentareacode') != '0') break;
        }
    }
}

// 初始化地域数据到控件
function init_areas() {
    var _roots = {};
    for (var i = 0; i < addr_data.length; i++) {
        try {
            if (addr_data[i].parentareacode == '0') {
                eval("_roots.a_" + addr_data[i].areacode + "={\"areaname\":\"" + addr_data[i].areaname + "\", \"arealevel\":\"" + addr_data[i].arealevel + "\", \"areacode\":\"" + addr_data[i].areacode + "\", \"parentareacode\":\"" + addr_data[i].parentareacode + "\"}");
                eval("_roots.a_" + addr_data[i].areacode + ".subs=[]");
            } else {
                eval("_roots.a_" + addr_data[i].parentareacode + ".subs.push({\"areaname\":\"" + addr_data[i].areaname + "\", \"arealevel\":\"" + addr_data[i].arealevel + "\", \"areacode\":\"" + addr_data[i].areacode + "\", \"parentareacode\":\"" + addr_data[i].parentareacode + "\"})");
            }
        } catch(e) {
            //alert(typeof addr_data);
            break;
        }
    }

    for (area in _roots) {
        //add_log(_roots[area].areaname);
        new_area(_roots[area].areacode, _roots[area].areaname, _roots[area].arealevel, _roots[area].parentareacode, false);
        if (_roots[area].subs[0]) {
            for (var i = 0; i < _roots[area].subs.length; i++) {
                //add_log('&nbsp;&nbsp;' + _roots[area].subs[i].areaname);
                new_area(_roots[area].subs[i].areacode, _roots[area].subs[i].areaname, _roots[area].subs[i].arealevel, _roots[area].subs[i].parentareacode, true);
            }
        }
    }
}
function init_interest() {
    if (!interest) {
        return;
    }
    var _roots = {};
    for (var i = 0; i < interest.length; i++) {
        try {
            if (interest[i].parentareacode == '0') {
                eval("_roots.a_" + interest[i].areacode + "={\"areaname\":\"" + interest[i].areaname + "\", \"arealevel\":\"" + interest[i].arealevel + "\", \"areacode\":\"" + interest[i].areacode + "\", \"parentareacode\":\"" + interest[i].parentareacode + "\"}");
                eval("_roots.a_" + interest[i].areacode + ".subs=[]");
            } else {
                eval("_roots.a_" + interest[i].parentareacode + ".subs.push({\"areaname\":\"" + interest[i].areaname + "\", \"arealevel\":\"" + interest[i].arealevel + "\", \"areacode\":\"" + interest[i].areacode + "\", \"parentareacode\":\"" + interest[i].parentareacode + "\"})");
            }
        } catch(e) {
            //alert(typeof interest);
            break;
        }
    }

    for (area in _roots) {
        //add_log(_roots[area].areaname);
        new_area(_roots[area].areacode, _roots[area].areaname, _roots[area].arealevel, _roots[area].parentareacode, false);
        if (_roots[area].subs[0]) {
            for (var i = 0; i < _roots[area].subs.length; i++) {
                //add_log('&nbsp;&nbsp;' + _roots[area].subs[i].areaname);
                new_area(_roots[area].subs[i].areacode, _roots[area].subs[i].areaname, _roots[area].subs[i].arealevel, _roots[area].subs[i].parentareacode, true);
            }
        }
    }
}
// 在type下新增项
function new_area(ac, an, al, pac, issub) {
    var _li = document.createElement('li');
    _li.id = "area_" + ac;
    _li.innerHTML = an;
    _li.setAttribute('areaid', ac);
    _li.setAttribute('arealevel', al);
    _li.setAttribute('parentareacode', pac);
    if (issub) _li.className = 'issub';
    _li.onclick = function(e) {
        var ev = ! e ? window.event: e;
        var _li = _jsc.evt.gTar(ev);
        if (_li.className.indexOf('added') > - 1) return;
        var __areas = $i('areas').getElementsByTagName('li');
        var isparentid = _li.getAttribute("arealevel") == "0" ? true: false;

        for (var j = 0; j < __areas.length; j++) {
            __areas[j].className = __areas[j].className.replace('selected', '');
            if (isparentid) {
                __areas[j].className = __areas[j].className.replace('show', '');
                if (_li.getAttribute("areaid") == __areas[j].getAttribute("parentareacode")) __areas[j].className += " show";
            }
        }
        _li.className += ' selected';
    }
    $i('areas').appendChild(_li);
}

/*if ($i('showeverywhere').checked) mask_area('area_more_sets');
  _jsc.util.addEvent(window,"load",function(){

  });*/
function modifyCpcTransAreas() {
    //alert($i('choosed_areas_name_flag').value);
    $i('areasform').submit();
}

