$(document).ready(function() {
	var keywordlen = '100';
	var choose_word_number = $('#word_areas').val().split('\n').length;

	//form绑定校验事件
	$("#add_new").validate();
	//推广计划下拉改变触发事件
	$('#ad_plan_id').change(function() {
		var plan_id = $(this).val();
		$('#ad_group_id').empty();
		$('#ad_group_id').html('<option value="">请选择推广组</option>');
		$.getJSON('/ad/searchgrouplist', {
			"plan_id": plan_id,
			"group_id": current_group_id,
			"type_id": 1
		},
		function(msg) {
			var _obj_size = msg.length;
			if (plan_id > 0 && _obj_size == 0) {
				if (confirm('该推广计划没有任何推广组，请先创建推广组？')) {
					window.location.href = "/adgroup/new?plan_id=" + plan_id;
				}
			} else {
				$.each(msg, function(i, item) {
					$('#ad_group_id').append('<option value=' + item.id + ' type=' + item.type + ' sizew=' + item.width + ' sizeh=' + item.height + '>' + item.title + '</option>');
				});
			}
		});
	});
	//点击新建创建图片创意事件
	$("#next_step").click(function() {
		var ad_plan_id = $('#ad_plan_id').val(),
		ad_group_id = $('#ad_group_id').val(),
		word_areas = $('#word_areas').val();
		$("#add_new").validate().form();
		if ($("#add_new").validate().checkForm() == true) {
			$.post("/ad/CheckAuditResult", {
				"word_areas": word_areas,
				"plan_id": ad_plan_id,
				"group_id": ad_group_id
			},
			function(data) { //异步校验是否重复
				if (data == 1) {
					$('#add_new').submit();
				} else if (data == 2) {
					alert('该创意已存在！');
					return false;
				} else {
					alert(data);
				}
			})
		}
	})
	$("#cancel_add").click(function() {
		window.history.go( - 1);
	});
	//键盘回车和删除关联关键词操作事件
	$('#word_areas').live('keyup keydown', function(event) {
		if (choose_word_number >= keywordlen && event.keyCode == 13) {
			return false;
		} else {
			if (!$(this).val()) {
				choose_word_number = 0;
				$('#choose_word_number').html(0);
			} else {
				/*if($(this).val().slice(-1) == '' || 'undefined'){
                        $(this).val($(this).val().slice(0,-1));
                    }*/
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
    //为表格添加排序功能
    $.tablesorter.addParser({
        id: "title",
        is: function(s) {
            return false;
        },
        format: function(s) {
            return s.replace("<","").replace(">","");;
        },
        type: "text",
    });
    //为表格添加排序功能
    $.tablesorter.addParser({
        id: "pv",
        is: function(s) {
            return false;
        },
        format: function(s) {
            return s.replace(/^\s*>\s*5000$/,"5001").replace(/^\s*&gt;\s*5000$/,"5001").replace(/^\s*<\s*50$/,"49").replace(/^\s*&lt;\s*50$/,"49").replace("undefined","0");
        },
        type: "numeric",
    });
    $("#list_tab").tablesorter({
        headers:{
            0:{"sorter":"pinyin"},
            1:{"sorter":"pv"}
        },
        cssAsc:"sortDown",
        cssDesc:"sortUp"        
    });
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
			$('#no_result').hide();
			$('#have_result').hide();
			$('#load_data').show();
			$('#choosed_word tr').removeClass('gray');
			var datahtml = '',
			arrword = '';
			$.getJSON('/adgroup/ajaxgetKeywordcainiInfo/', {
				'search_word': search_word
			},
			function(data) {
				if (!data) {
					datahtml = '<tr><td colspan="3" align="center">无查询结果，请搜索其它关键词</td></tr>';
				} else {
					$.each(data.kr_res, function(i, item) {
						datahtml += '<tr><td class="result_word">' + item.word + '</td><td>' + item.pv + '</td><td><span class="sche"><em style="width:' + item.compete + '%;">' + item.compete + '</em></span></td></tr>';
					})
				}
				$('#choosed_word').html(datahtml);
                $("#list_tab").trigger("update");//表格数据发生变化，需要通知排序组件
				$('#load_data').hide();
				$('#have_result').show();
			})
		}
	});
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
		}
		else {
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
		$('#choosed_word tr').not('[class=gray]').slice(0, all_number).click();
	})
	//点击保存关键词操作事件
	$('#save_choose_word').click(function() {
		if (choose_word_number > keywordlen) {
			alert('您选择的关键词数量超出最大限值！');
			return false;
		}
	})
	$('.close_box').click(function() {
		$('#mask').remove();
		$('#keyword_box').hide();
	})
	$('#choosed_areas li').click(function() {
		$('#choosed_areas li').removeClass('selected');
		$(this).addClass('selected');
	})
});
function modify() {
	document.getElementById("mbox").style.display = "block";
	mask();
};
function clos() {
	var x = document.getElementById("mask");
	x.parentNode.removeChild(x);
	document.getElementById("mbox").style.display = "none";
}

