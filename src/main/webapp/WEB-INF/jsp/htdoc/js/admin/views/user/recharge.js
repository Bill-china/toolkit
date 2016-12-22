$(function() {
	$("select[name=pay_type]").change(function() {
		if (this.value == - 8 && $(this).parent().find(".pay_time").length == 0) {
            var userid = $(this).parent().find("input[name=userid]").val();
            var thisnode = $(this);
            $.get('recharge?mod=getStartTime&userid='+userid,function(data){
                var container = $("<div class='pay_time' style='text-align:left; margin:5px; line-height:24px;'></div>"),
                start = $("<div class='timeStart'>开始时间:<select class='pay_time_year' name='pay_time_year'></select><select class='pay_time_month' name='pay_time_month'></select></div>"),
                end = $("<div class='timeEnd'>结束时间:<input class='pay_time_end' style='width:80px;' name='pay_time_end' type='text' disabled='disabled' /></div>"),
                i,
                time,
                year,
                month,
                endTime,
                curTime,
                timeYear = start.find("select[name=pay_time_year]"),
                timeMonth = start.find("select[name=pay_time_month]");
                for (i = 2011; i < 2020; i++) {
                    timeYear.get(0).add(new Option(i, i));
                }
                for (i = 1; i < 13; i++) {
                    timeMonth.get(0).add(new Option(i, i));
                }
                if(data == 0){
                    time = new Date();
                    year = time.getFullYear();
                    month = time.getMonth() + 1;
                }else{
                    year = data.year;
                    month = data.month;
                }
                curTime = getMonthOwn(year, month, 1);
                timeYear.change(function() {
                    endTime = getMonthOwn(timeYear.val(), timeMonth.val(), 11);
                    end.find("input[name=pay_time_end]").val(endTime.year + "年" + endTime.month + "月");
                }).val(curTime.year).change();
                timeMonth.change(function() {
                    endTime = getMonthOwn(timeYear.val(), timeMonth.val(), 11);
                    end.find("input[name=pay_time_end]").val(endTime.year + "年" + endTime.month + "月");
                }).val(curTime.month).change();

                container.append(start);
                container.append(end);
                thisnode.parent().append(container);
                thisnode.parent().nextAll('td').find('.money').eq(0).val(-600);
            },'json');
        }else if (this.value == -11){
            var userid = $(this).parent().find("input[name=userid]").val();
            var thisnode = $(this);
            $.get('recharge?mod=getAmount&userid='+userid,function(data){
                thisnode.parent().nextAll('td').find('.money').eq(0).val(data).attr('readonly', 'readonly');
            });
		} else if (this.value != - 8 && $(this).parent().find(".pay_time").length != 0) {
			$(this).parent().find(".pay_time").remove();
            $(this).parent().nextAll('td').find('.money').eq(0).val('');
        }
	});
	function getMonthOwn(year, month, delay) {
		var year = parseInt(year),
		month = parseInt(month),
		n = delay;
		month = month + n;
        if(month > 12){
            year = year + Math.floor(month / 12);
            month = month % 12;
        }
		return {
			year: year,
			month: month
		}
	}
	$('.payfor').click(function() {
		var money = $.trim($(this).prev('.money').val());
		var userId = $(this).attr('name');
		var payType = $(this).closest('tr').find('.pay_type').val();
		var newesg = /^\d+$/;
        var dom_btn = $(this); 
		if (!money || money == 0) {
			Boxy.alert("金额为大于0的整数！", null, {
				title: "错误提示"
			});
			return false;
		}
		if (!userId) {
			Boxy.alert('错误的用户ID');
			return false;
		}
		if (payType == 0) {
			Boxy.alert('请选择充值渠道');
			return false;
        } else if(payType == -8){
			Boxy.confirm('确定要充值吗?', function() {
                var time_year= parseInt(dom_btn.closest('tr').find('.pay_time_year').val());
                var time_month = parseInt(dom_btn.closest('tr').find('.pay_time_month').val());
                var time_start = time_year * 100 + time_month;
                
				$.ajax({
					url: '/admin/user/recharge',
					type: 'post',
					data: {
						'money': money,
						'pay_type': payType,
						'admin_id': userId,
                        'start_month':time_start
					},
					success: function(text) {
						Boxy.alert('充值成功！', function() {
							window.location.href = "/admin/user/listrecharge?user_id=" + userId;
						});
					}
				});
			})
		} else {
			Boxy.confirm('确定要充值吗?', function() {
				$.ajax({
					url: '/admin/user/recharge',
					type: 'post',
					data: {
						'money': money,
						'pay_type': payType,
						'admin_id': userId
					},
					success: function(text) {
						Boxy.alert('充值成功！', function() {
							window.location.href = "/admin/user/listrecharge?user_id=" + userId;
						});
					}
				});
			})
		}
	});
})

