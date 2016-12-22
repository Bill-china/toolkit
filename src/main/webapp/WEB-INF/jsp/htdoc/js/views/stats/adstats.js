$(function() {
    (function(){
        $("#data_tab").tablesorter({
            cssAsc:"sortDown",
            cssDesc:"sortUp"        
        });
    })();
	var tfootlen = $('#tfoot').children('td').length,
	ptlen = $('#headtr th').length;
	var charlen = ptlen - tfootlen;
	$('.date_type_label').eq(3).hide();
	if ($('.ceng_type:checked').val() == 5) {
		$('#report_type').hide();
	}
	$('#tfoot td:first').attr('colspan', charlen + 1);
	$('#tfoot td:last').css('border-right', '1px solid #E5E4E7');
	if (day_check == 'self') {
		$('#date_box').show();
	} else {
		$('#date_box').hide();
	}
	if (ceng_check == 'self') {
		$('#ceng_box').show();
	} else {
		$('#ceng_box').hide();
	}
	$('.date_type').click(function() {
		if (this.value == 'self') {
			$('#date_box').show();
		} else {
			$('#date_box').hide();
		}
	})
	$('.ceng_type').click(function() {
		if (this.value == 'self') {
			$('#ceng_box').show();
		} else {
			$('#ceng_box').hide();
		}
		if (this.value == '5') {
			$('#report_type').hide();
		} else {
			$('#report_type').show();
		}
	})
	$('input[name = show_check]:first').prop('checked', 'true');
	$('#end_date').val(end_date);
	$('input[name="baogao_check"]').click(function() {
		$('#error_info').html('').hide();
	})
	$('#next_step').click(function() {
		if ($('.date_type:checked').val() == "self") {
			var start_date = $("#start_date").val(),
			end_date = $("#end_date").val(),
			start_date_data = start_date.replace(/\-/g, ""),
			end_date_data = end_date.replace(/\-/g, "");
			if (!start_date || ! end_date) {
				$('#error_info').html('请选择时间').show();
				return false;
			}
			if (end_date < start_date) {
				$('#error_info').html('开始时间不能大于结束时间').show();
				return false;
			}
			if ($('input[name="baogao_check"]:checked').val() == 2 && (end_date_data - start_date_data > 7)) {
				$('#error_info').html('地域报告暂仅支持最长7天的数据统计').show();
				return false;
			}
		}
	})
	var dates = $('#start_date,#end_date').datepick({
		showOnFocus: true,
		dateFormat: 'yy-mm-dd',
		minDate: new Date(2012, 6 - 1, 1),
		maxDate: '-1D',
		beforeShow: function() {
			$('#error_info').html('').hide();
		},
		onSelect: function(selectedDate) {
			var option = this.id == 'start_date' ? 'minDate': 'maxDate';
			var instance = $(this).data("datepick");
			var date = $.datepick.parseDate(
			instance.settings.dateFormat || $.datepick._defaults.dateFormat, selectedDate, instance.settings);
			dates.not(this).datepick('option', option, date);
		}
	});
	$('.slt').change(function() {
		$('#date_form').submit();
	});
	//图表
});

function exportFile() {
	var url = document.location.href;
	if (url.indexOf('?') > 0 && url.indexOf('export=') == - 1) {
		url += '&export=1';
	} else {
		url += '?export=1';
	}
	document.location.href = url;
}

